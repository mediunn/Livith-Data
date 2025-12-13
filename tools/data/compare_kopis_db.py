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
- AI를 사용한 아티스트명 추출
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

from tqdm import tqdm
from lib.discord_notifier import DiscordNotifier

# 프로젝트 루트 경로를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.apis.kopis_api import KopisAPI
from core.apis.gemini_api import GeminiAPI
from lib.db_utils import get_db_manager
from lib.config import Config
from lib.prompts import DataCollectionPrompts

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


def extract_artist_from_title(gemini_api: GeminiAPI, title: str, kopis_artist: str = '') -> str:
    """
    AI를 사용해 콘서트 제목에서 아티스트 이름 추출
    prompts.py의 get_artist_name_prompt 사용 (추출+검증 한번에)
    """
    # KOPIS artist가 이미 "원어 (한국어)" 형식이면 그대로 사용
    if kopis_artist and '(' in kopis_artist and ')' in kopis_artist:
        return kopis_artist
    
    try:
        prompt = DataCollectionPrompts.get_artist_name_prompt(title)
        response = gemini_api.query_json(prompt, use_search=True)
        artist = response.get('artist', '')
        
        # 빈 값이나 의심스러운 응답 필터링
        invalid_responses = ['unknown', '알 수 없음', 'n/a', 'none', 'various artists', '아티스트명', '정보 없음']
        if not artist or artist.lower() in invalid_responses:
            return kopis_artist if kopis_artist else ''
        
        # "원어 (한국어)" 형식 확인
        if '(' not in artist or ')' not in artist:
            return kopis_artist if kopis_artist else ''
        
        return artist
    except Exception as e:
        logger.warning(f"아티스트 추출 실패 ({title}): {e}")
        return kopis_artist if kopis_artist else ''


# 전역 rate limiter (초당 10회 요청)
rate_limiter = RateLimiter(calls_per_second=10)


def is_visit_concert(detail: Dict[str, Any]) -> tuple[bool, bool]:
    """
    내한공연 여부 확인
    Returns: (is_valid, is_jazz) - 유효한 내한공연 여부, 내한공연 중 재즈 여부
    """
    title = detail.get('title', '')
    
    is_visit = (
        detail.get('visit') == 'Y' and 
        detail.get('festival') == 'N' and
        bool(title) and 
        bool(detail.get('artist'))
    )
    
    # 내한공연이 아니면 둘 다 False
    if not is_visit:
        return False, False
    
    # 내한공연 중 재즈인지 체크
    is_jazz = '재즈' in title
    
    return (not is_jazz, is_jazz)


def normalize_date(date_str: str) -> str:
    """날짜 형식 정규화 (YYYYMMDD -> YYYY.MM.DD)"""
    if date_str and len(date_str) == 8 and date_str.isdigit():
        return f"{date_str[:4]}.{date_str[4:6]}.{date_str[6:8]}"
    return date_str


def fetch_single_concert(code: str, api: KopisAPI) -> tuple[Optional[Dict[str, Any]], bool]:
    """
    단일 공연 정보를 가져오는 함수
    Returns: (detail or None, is_jazz)
    """
    try:
        rate_limiter.wait()
        detail = api.get_concert_detail(code)
        if detail:
            is_visit, is_jazz = is_visit_concert(detail)
            if is_visit:
                detail['start_date'] = normalize_date(detail.get('start_date', ''))
                detail['end_date'] = normalize_date(detail.get('end_date', ''))
                return detail, False
            return None, is_jazz
        return None, False
    except Exception as e:
        logger.warning(f"공연 코드 {code} 처리 실패: {e}")
        return None, False


def fetch_concerts_parallel(
    concert_codes: List[str], 
    api: KopisAPI, 
    max_workers: int = 20
) -> tuple[List[Dict[str, Any]], int]:
    """
    병렬 처리로 공연 정보를 가져오는 함수
    
    Rate limiter로 초당 요청 수를 제한하여 API 차단 방지
    Returns: (concert_list, jazz_count)
    """
    result = []
    jazz_count = 0
    fetch_func = partial(fetch_single_concert, api=api)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_code = {
            executor.submit(fetch_func, code): code 
            for code in concert_codes
        }
        
        with tqdm(total=len(concert_codes), desc="내한공연 필터링") as pbar:
            for future in as_completed(future_to_code):
                detail, is_jazz = future.result()
                if detail:
                    result.append(detail)
                if is_jazz:
                    jazz_count += 1
                pbar.update(1)
    
    return result, jazz_count


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
    print(f"   아티스트: {details.get('artist', '아티스트 없음')}")
    print(f"   기간: {details.get('start_date', 'N/A')} ~ {details.get('end_date', 'N/A')}")


def print_comparison_results(
    kopis_codes: set, 
    db_codes: set, 
    kopis_concerts: Dict, 
    db_concerts: Dict,
    jazz_count: int = 0,
    gemini_api: Optional[GeminiAPI] = None
):
    """비교 결과 출력"""
    new_codes = kopis_codes - db_codes
    removed_codes = db_codes - kopis_codes
    
    total_kopis = len(kopis_codes) + jazz_count  # 재즈 포함 전체 내한공연 수
    
    print("\n" + "=" * 80)
    print("🔍 KOPIS vs DB 비교 결과 (내한 공연 기준)")
    print("=" * 80)
    print(f"📊 통계:")
    print(f"   - KOPIS 내한 공연: {total_kopis}개")
    print(f"   - DB 현재/미래 공연: {len(db_codes)}개")
    if jazz_count > 0:
        print(f"   - 🎷 재즈 공연 (제외됨): {jazz_count}개")
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
        
        # AI로 아티스트명 추출
        if gemini_api:
            print("\n🤖 AI로 아티스트명 추출 중 (2단계 검증)...")
            for code in tqdm(sorted(new_codes), desc="아티스트 추출"):
                details = kopis_concerts.get(code, {})
                title = details.get('title', '')
                kopis_artist = details.get('artist', '')
                if title:
                    extracted_artist = extract_artist_from_title(gemini_api, title, kopis_artist)
                    if extracted_artist:
                        details['artist'] = extracted_artist
            print("✅ 아티스트명 추출 완료\n")
        
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


def compare_concerts():
    """KOPIS와 DB의 콘서트 목록을 비교하고 차이점을 출력"""
    kopis_api = KopisAPI(api_key=Config.KOPIS_API_KEY)
    db_manager = get_db_manager()
    
    # AI 아티스트 추출을 위한 GeminiAPI 초기화
    gemini_api = GeminiAPI(api_key=Config.GEMINI_API_KEY)

    try:
        logger.info("=" * 50)
        logger.info("🎵 공연 데이터 동기화 검사 시작")
        logger.info("=" * 50)
        
        # 1. 데이터베이스 연결
        logger.info("데이터베이스에 연결하는 중...")
        if not db_manager.connect_with_ssh():
            logger.error("❌ 데이터베이스 연결에 실패했습니다.")
            return

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
            return
        
        max_db_date_str = max_date_result[0]
        max_db_date = datetime.strptime(max_db_date_str, "%Y.%m.%d")
        end_date_str = max_db_date.strftime("%Y%m%d")
        
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
            return
        
        if not all_kopis_codes:
            logger.warning("⚠️ KOPIS에서 가져온 공연이 없습니다.")
            return
        
        # 4. 내한 공연 필터링 (병렬 처리)
        logger.info(f"\n🔍 내한 공연 필터링 중 (병렬 처리, 동시 작업: 20개)...")
        try:
            concert_details, jazz_count = fetch_concerts_parallel(
                all_kopis_codes, 
                api=kopis_api, 
                max_workers=20
            )
            
            kopis_concerts = {
                detail['code']: detail for detail in concert_details
            }
            kopis_codes = set(kopis_concerts.keys())
            logger.info(f"✅ KOPIS에서 {len(kopis_codes)}개의 내한 공연을 찾았습니다.")
        except Exception as e:
            logger.error(f"❌ 공연 상세 정보 가져오기 중 오류 발생: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return

        # 5. 데이터베이스에서 공연 조회
        logger.info(f"\n💾 데이터베이스에서 공연 목록을 가져오는 중...")
        try:
            db_concerts = get_db_concerts(db_manager, today_for_db, max_db_date_str)
            db_codes = set(db_concerts.keys())
            logger.info(f"✅ 데이터베이스에서 {len(db_codes)}개의 공연을 찾았습니다.")
        except Exception as e:
            logger.error(f"❌ 데이터베이스 조회 중 오류 발생: {e}")
            return

        # 6. 결과 출력 (AI 아티스트 추출 포함)
        logger.info("\n🔄 공연 목록 비교 중...")
        print_comparison_results(
            kopis_codes, db_codes, kopis_concerts, db_concerts, 
            jazz_count, gemini_api
        )

        # 7. Discord 알림 전송 (try 블록 안에 있어야 함!)
        if Config.DISCORD_WEBHOOK_URL:
            logger.info("📤 Discord 알림 전송 중...")
            notifier = DiscordNotifier(Config.DISCORD_WEBHOOK_URL)
            if notifier.send_compare_result(
                kopis_codes, db_codes, kopis_concerts, db_concerts, jazz_count
            ):
                logger.info("✅ Discord 알림 전송 완료")
            else:
                logger.warning("⚠️ Discord 알림 전송 실패")

    except Exception as e:
        logger.error(f"❌ 비교 작업 중 예상치 못한 오류 발생: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        if db_manager:
            db_manager.disconnect()
            logger.info("\n🔌 데이터베이스 연결을 종료했습니다.")

if __name__ == "__main__":
    compare_concerts()