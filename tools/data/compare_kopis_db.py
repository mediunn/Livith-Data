#!/usr/bin/env python3
"""
KOPIS API와 로컬 데이터베이스의 콘서트 목록을 비교하여
새로 추가되거나 사라진 공연을 찾아 출력하는 스크립트

개선 사항:
- 현재/미래 공연만 비교하도록 날짜 필터링 강화
- 병렬 처리로 속도 개선 (rate limit 고려)
- 전역 변수 제거, 의존성 주입 방식으로 변경
- 에러 처리 개선
- 로깅 상세화
"""
import os
import sys
import time
import logging
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from typing import Dict, List, Optional, Any
from calendar import monthrange

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from tqdm import tqdm
from lib.discord_notifier import DiscordNotifier

from core.apis.kopis_api import KopisAPI
from core.apis.gemini_api import GeminiAPI
from lib.db_utils import get_db_manager
from lib.config import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RateLimiter:
    """스레드 안전한 Rate Limiter"""
    def __init__(self, calls_per_second: float):
        self.min_interval = 1.0 / calls_per_second
        self.lock = threading.Lock()
        self.last_call = 0
    
    def wait(self):
        with self.lock:
            now = time.time()
            wait_time = self.min_interval - (now - self.last_call)
            if wait_time > 0:
                time.sleep(wait_time)
            self.last_call = time.time()


# 전역 rate limiter (초당 10회 요청)
rate_limiter = RateLimiter(calls_per_second=10)

# 제외할 장르 키워드 (제목에 포함되면 제외)
EXCLUDED_GENRES = [
    '재즈', '클래식',
    '연주회', '독주회',
    '기타리스트', '핑거스타일',
    '앙상블', '챔버', '콰르텟', '트리오',
    '관현악', '교향',
]


def validate_config():
    """필수 설정 검증"""
    required = ['KOPIS_API_KEY']
    missing = [key for key in required if not getattr(Config, key, None)]
    if missing:
        raise ValueError(f"필수 설정 누락: {missing}")


def is_visit_concert(detail: Dict[str, Any]) -> tuple[bool, str]:
    """
    내한공연 여부 확인
    Returns: (is_valid, excluded_genre) - 유효한 내한공연 여부, 제외된 장르명 (없으면 빈 문자열)
    """
    title = detail.get('title', '')

    is_visit = (
        detail.get('visit') == 'Y' and
        detail.get('festival') == 'N' and
        bool(title) and
        bool(detail.get('artist'))
    )

    # 내한공연이 아니면 통과
    if not is_visit:
        return False, ''

    # 내한공연 중 제외 장르 체크
    for genre in EXCLUDED_GENRES:
        if genre in title:
            return False, genre

    return True, ''


def normalize_date(date_str: str) -> str:
    """날짜 형식 정규화 (YYYYMMDD -> YYYY.MM.DD)"""
    if date_str and len(date_str) == 8 and date_str.isdigit():
        return f"{date_str[:4]}.{date_str[4:6]}.{date_str[6:8]}"
    return date_str


def fetch_single_concert(code: str, api: KopisAPI, max_retries: int = 3) -> tuple[Optional[Dict[str, Any]], str]:
    """
    단일 공연 정보를 가져오는 함수 (400 에러 시 재시도)
    Returns: (detail or None, excluded_genre)
    """
    for attempt in range(max_retries):
        try:
            rate_limiter.wait()
            detail = api.get_concert_detail(code)
            if detail:
                is_valid, excluded_genre = is_visit_concert(detail)
                if is_valid:
                    detail['start_date'] = normalize_date(detail.get('start_date', ''))
                    detail['end_date'] = normalize_date(detail.get('end_date', ''))
                    return detail, ''
                return None, excluded_genre
            # detail이 None (400 에러 등) → 재시도
            if attempt < max_retries - 1:
                time.sleep(1 * (attempt + 1))
                continue
            return None, ''
        except Exception as e:
            logger.warning(f"공연 코드 {code} 처리 실패 (시도 {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(1 * (attempt + 1))
                continue
            return None, ''
    return None, ''


def fetch_concerts_parallel(
    concert_codes: List[str],
    api: KopisAPI,
    max_workers: int = 10
) -> tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    병렬 처리로 공연 정보를 가져오는 함수

    Rate limiter로 초당 요청 수를 제한하여 API 차단 방지
    Returns: (concert_list, excluded_counts)
    """
    result = []
    excluded_counts: Dict[str, int] = {genre: 0 for genre in EXCLUDED_GENRES}
    fetch_func = partial(fetch_single_concert, api=api)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_code = {
            executor.submit(fetch_func, code): code
            for code in concert_codes
        }

        with tqdm(total=len(concert_codes), desc="내한공연 필터링") as pbar:
            for future in as_completed(future_to_code):
                detail, excluded_genre = future.result()
                if detail:
                    result.append(detail)
                if excluded_genre:
                    excluded_counts[excluded_genre] = excluded_counts.get(excluded_genre, 0) + 1
                pbar.update(1)

    return result, excluded_counts


def filter_instrumental_with_gemini(
    concerts: List[Dict[str, Any]],
    gemini_api: GeminiAPI,
    delay: float = 2.0
) -> tuple[List[Dict[str, Any]], int]:
    """
    Gemini AI + Google Search로 연주곡(기악) 공연 필터링
    Returns: (filtered_concerts, ai_excluded_count)
    """
    if not concerts:
        return concerts, 0

    filtered = []
    excluded_count = 0

    with tqdm(total=len(concerts), desc="AI 연주곡 필터링") as pbar:
        for concert in concerts:
            title = concert.get('title', '')
            artist = concert.get('artist', '')

            prompt = f"""다음 공연이 가사 없는 연주곡/기악 공연인지 판단해줘.

제목: {title}
아티스트: {artist}

이 아티스트를 검색해서, 주로 기악 연주 위주(피아노·기타·바이올린 등 가사 없는 연주)인지,
아니면 보컬/노래가 있는 아티스트인지 확인해줘.

딱 한 단어만 답해:
- 연주곡 (기악 연주 위주, 가사 없음)
- 보컬 (노래/보컬 있음)"""

            try:
                response = gemini_api.query(prompt, use_search=True).strip()
                if '연주곡' in response and '보컬' not in response:
                    excluded_count += 1
                    logger.info(f"AI 연주곡 제외: [{concert.get('code')}] {title}")
                else:
                    filtered.append(concert)
            except Exception as e:
                logger.warning(f"AI 판단 실패, 포함 처리: {title} - {e}")
                filtered.append(concert)

            pbar.update(1)
            if delay > 0:
                time.sleep(delay)

    return filtered, excluded_count


def get_db_concerts(db_manager, start_date: str, end_date: str) -> Dict[str, Dict[str, Any]]:
    """데이터베이스에서 공연 목록 조회"""
    db_manager.cursor.execute(
        """
        SELECT code, title, artist, start_date, end_date, venue
        FROM concerts 
        WHERE end_date >= %s AND end_date <= %s
        ORDER BY start_date
        """,
        (start_date, end_date)
    )
    
    results = db_manager.cursor.fetchall()
    return {
        row[0]: {
            'title': row[1],
            'artist': row[2],
            'start_date': row[3],
            'end_date': row[4],
            'venue': row[5],
        } for row in results
    }


def print_concert_info(idx: int, code: str, details: Dict[str, Any]):
    """공연 정보 출력"""
    print(f"\n{idx}. 공연 코드: {code}")
    print(f"   제목: {details.get('title', '제목 없음')}")
    print(f"   기간: {details.get('start_date', 'N/A')} ~ {details.get('end_date', 'N/A')}")


def print_comparison_results(
    kopis_codes: set,
    db_codes: set,
    kopis_concerts: Dict,
    db_concerts: Dict,
    excluded_counts: Dict[str, int] = None
):
    """비교 결과 출력"""
    excluded_counts = excluded_counts or {}
    new_codes = kopis_codes - db_codes
    removed_codes = db_codes - kopis_codes

    total_excluded = sum(excluded_counts.values())
    total_kopis = len(kopis_codes) + total_excluded

    print("\n" + "=" * 80)
    print("🔍 KOPIS vs DB 비교 결과 (내한 공연 기준)")
    print("=" * 80)
    print(f"📊 통계:")
    print(f"   - KOPIS 내한 공연: {total_kopis}개")
    print(f"   - DB 현재/미래 공연: {len(db_codes)}개")
    for genre, count in sorted(excluded_counts.items()):
        print(f"   - {genre} 공연 (제외됨): {count}개")
    print(f"   - 새로 추가된 공연: {len(new_codes)}개")
    print(f"   - 사라진 공연: {len(removed_codes)}개")
    
    # 월별 새로 추가된 공연 통계
    if new_codes:
        monthly_stats = {}
        for code in new_codes:
            details = kopis_concerts.get(code, {})
            start_date = details.get('start_date', '')
            if start_date and len(start_date) >= 7:
                month_key = start_date[:7]  # "YYYY.MM"
                monthly_stats[month_key] = monthly_stats.get(month_key, 0) + 1
        
        if monthly_stats:
            print(f"\n   📅 월별 새로 추가된 공연:")
            for month in sorted(monthly_stats.keys()):
                print(f"      - {month}: {monthly_stats[month]}개")
    
    print("=" * 80)

    if not new_codes and not removed_codes:
        print("\n✅ 변경 사항이 없습니다. KOPIS와 데이터베이스가 완전히 동기화되어 있습니다.")
        return
    
    # 새로 추가된 공연
    if new_codes:
        print(f"\n{'=' * 80}")
        print(f"✨ 새로 추가된 공연 (KOPIS에는 있지만 DB에는 없음) - {len(new_codes)}개")
        print(f"{'=' * 80}")
        
        for idx, code in enumerate(sorted(new_codes), 1):
            print_concert_info(idx, code, kopis_concerts.get(code, {}))
    
    # 사라진 공연
    if removed_codes:
        print(f"\n{'=' * 80}")
        print(f"🗑️  사라진 공연 (DB에는 있지만 KOPIS에는 없음) - {len(removed_codes)}개")
        print(f"{'=' * 80}")
        print("⚠️  주의: 공연 취소 또는 KOPIS에서 삭제된 공연일 수 있습니다.")
        print(f"{'=' * 80}")
        for idx, code in enumerate(sorted(removed_codes), 1):
            print_concert_info(idx, code, db_concerts.get(code, {}))
    
    print("\n" + "=" * 80)
    print("✅ 비교 작업 완료")
    print("=" * 80)


def compare_concerts() -> dict:
    """KOPIS와 DB의 콘서트 목록을 비교하고 차이점을 출력"""
    
    # 필수 설정 검증
    validate_config()
    
    # 실행 시간 측정 시작
    start_time = time.time()
    
    kopis_api = KopisAPI(api_key=Config.KOPIS_API_KEY)
    db_manager = get_db_manager()
    
    result = {
        'new_count': 0,
        'removed_count': 0,
        'elapsed_time': 0,
        'success': False
    }

    try:
        logger.info("=" * 50)
        logger.info("🎵 공연 데이터 동기화 검사 시작")
        logger.info("=" * 50)
        
        # 1. 데이터베이스 연결
        logger.info("데이터베이스에 연결하는 중...")
        if not db_manager.connect_with_ssh():
            logger.error("❌ 데이터베이스 연결에 실패했습니다.")
            return result

        # 2. 날짜 범위 설정
        today = datetime.now()
        today_str = today.strftime("%Y%m%d")
        today_for_db = today.strftime("%Y.%m.%d")
        
        logger.info("데이터베이스에서 저장된 공연의 최대 종료일을 확인하는 중...")
        db_manager.cursor.execute(
            "SELECT MAX(end_date) FROM concerts WHERE end_date >= %s", 
            (today_for_db,)
        )
        max_date_result = db_manager.cursor.fetchone()
        
        if not max_date_result or not max_date_result[0]:
            logger.error("❌ 데이터베이스에 공연 정보가 없습니다.")
            return result
        
        max_db_date_str = max_date_result[0]
        max_db_date = datetime.strptime(max_db_date_str, "%Y.%m.%d")

        last_day = monthrange(max_db_date.year, max_db_date.month)[1]
        end_of_month = max_db_date.replace(day=last_day)
        end_date_str = end_of_month.strftime("%Y%m%d")
        max_db_date_str = end_of_month.strftime("%Y.%m.%d")
        
        logger.info(f"✅ 비교 기간: {today_str} ~ {end_date_str}")

        # 3. KOPIS API에서 공연 코드 목록 가져오기
        logger.info(f"\n📡 KOPIS API에서 공연 목록을 가져오는 중...")
        try:
            all_kopis_codes = kopis_api.fetch_all_concerts(
                start_date=today_str,
                end_date=end_date_str
            )
            logger.info(f"✅ KOPIS에서 총 {len(all_kopis_codes)}개의 공연을 찾았습니다.")
        except Exception as e:
            logger.error(f"❌ KOPIS API 호출 중 오류 발생: {e}")
            return result
        
        if not all_kopis_codes:
            logger.warning("⚠️ KOPIS에서 가져온 공연이 없습니다.")
            return result
        
        # 4. 내한 공연 필터링 (병렬 처리)
        logger.info(f"\n🔍 내한 공연 필터링 중 (병렬 처리, 동시 작업: 20개)...")
        try:
            concert_details, excluded_counts = fetch_concerts_parallel(
                all_kopis_codes,
                api=kopis_api,
                max_workers=20
            )
            
            logger.info(f"✅ KOPIS에서 {len(concert_details)}개의 내한 공연을 찾았습니다.")
        except Exception as e:
            logger.error(f"❌ 공연 상세 정보 가져오기 중 오류 발생: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return result

        # 5. Gemini AI로 연주곡 필터링
        if Config.GEMINI_API_KEY:
            logger.info(f"\n🤖 Gemini AI로 연주곡 필터링 중 ({len(concert_details)}개)...")
            try:
                gemini_api = GeminiAPI(api_key=Config.GEMINI_API_KEY)
                concert_details, ai_excluded_count = filter_instrumental_with_gemini(concert_details, gemini_api)
                excluded_counts['연주곡 (AI)'] = ai_excluded_count
                logger.info(f"✅ AI 필터링 후 {len(concert_details)}개 남음 (AI 제외: {ai_excluded_count}개)")
            except Exception as e:
                logger.warning(f"⚠️ AI 필터링 실패, 스킵: {e}")
        else:
            logger.info("⚠️ GEMINI_API_KEY 없음 - AI 연주곡 필터링 스킵")

        kopis_concerts = {detail['code']: detail for detail in concert_details}
        kopis_codes = set(kopis_concerts.keys())
        logger.info(f"✅ 최종 {len(kopis_codes)}개의 공연으로 비교 진행")

        # 6. 데이터베이스에서 공연 조회
        logger.info(f"\n💾 데이터베이스에서 공연 목록을 가져오는 중...")
        try:
            db_concerts = get_db_concerts(db_manager, today_for_db, max_db_date_str)
            db_codes = set(db_concerts.keys())
            logger.info(f"✅ 데이터베이스에서 {len(db_codes)}개의 공연을 찾았습니다.")
        except Exception as e:
            logger.error(f"❌ 데이터베이스 조회 중 오류 발생: {e}")
            return result

        # 7. 결과 출력
        logger.info("\n🔄 공연 목록 비교 중...")
        print_comparison_results(
            kopis_codes, db_codes, kopis_concerts, db_concerts,
            excluded_counts
        )
        
        # 결과 저장
        new_codes = kopis_codes - db_codes
        removed_codes = db_codes - kopis_codes
        result['new_count'] = len(new_codes)
        result['removed_count'] = len(removed_codes)
        result['success'] = True

        # Discord 알림 전송 (재시도 로직 포함)
        if Config.DISCORD_WEBHOOK_URL:
            logger.info("📤 Discord 알림 전송 중...")
            notifier = DiscordNotifier(Config.DISCORD_WEBHOOK_URL)
            
            max_retries = 3
            for attempt in range(max_retries):
                if notifier.send_compare_result(
                    kopis_codes, db_codes, kopis_concerts, db_concerts, excluded_counts,
                    start_date=today_for_db,
                    end_date=max_db_date_str
                ):
                    logger.info("✅ Discord 알림 전송 완료")
                    break
                logger.warning(f"⚠️ Discord 알림 전송 실패 (시도 {attempt + 1}/{max_retries})")
                time.sleep(2)

    except Exception as e:
        logger.error(f"❌ 비교 작업 중 예상치 못한 오류 발생: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        if db_manager:
            db_manager.disconnect()
            logger.info("\n🔌 데이터베이스 연결을 종료했습니다.")
        
        # 실행 시간 측정 종료
        elapsed = time.time() - start_time
        result['elapsed_time'] = elapsed
        logger.info(f"⏱️ 총 소요 시간: {elapsed:.1f}초")
    
    return result


if __name__ == "__main__":
    compare_concerts()