#!/usr/bin/env python3
"""
KOPIS API와 로컬 데이터베이스의 콘서트 목록을 비교하는 테스트용 스크립트
- Discord 알림 없음
- 키워드 기반 장르 필터링 (재즈/클래식 등, AI 필터링은 없음)
- 빠른 검수용
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
from core.apis.kopis_api import KopisAPI
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


rate_limiter = RateLimiter(calls_per_second=10)

# 제외할 장르 키워드 (제목에 포함되면 제외)
EXCLUDED_GENRES = [
    '재즈', '클래식',
    '연주회', '독주회',
    '기타리스트', '핑거스타일',
    '앙상블', '챔버', '콰르텟', '트리오',
    '관현악', '교향',
    '피아노',
    '오케스트라', '바이올린',
]


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

    if not is_visit:
        return False, ''

    for genre in EXCLUDED_GENRES:
        if genre in title:
            return False, genre

    return True, ''


def normalize_date(date_str: str) -> str:
    """날짜 형식 정규화"""
    if date_str and len(date_str) == 8 and date_str.isdigit():
        return f"{date_str[:4]}.{date_str[4:6]}.{date_str[6:8]}"
    return date_str


def fetch_single_concert(code: str, api: KopisAPI) -> tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], str]:
    """
    단일 공연 정보를 가져오는 함수
    Returns: (valid_detail, excluded_detail, excluded_genre)
    """
    try:
        rate_limiter.wait()
        detail = api.get_concert_detail(code)
        if detail:
            is_valid, excluded_genre = is_visit_concert(detail)
            detail['start_date'] = normalize_date(detail.get('start_date', ''))
            detail['end_date'] = normalize_date(detail.get('end_date', ''))
            if is_valid:
                return detail, None, ''
            elif excluded_genre:
                return None, detail, excluded_genre
            return None, None, ''
        return None, None, ''
    except Exception as e:
        logger.warning(f"공연 코드 {code} 처리 실패: {e}")
        return None, None, ''


def fetch_concerts_parallel(
    concert_codes: List[str], api: KopisAPI, max_workers: int = 20
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
    """
    병렬 처리로 공연 정보를 가져오는 함수
    Returns: (valid_concerts, excluded_concerts, excluded_counts)
    """
    valid_concerts = []
    excluded_concerts = []
    excluded_counts: Dict[str, int] = {genre: 0 for genre in EXCLUDED_GENRES}
    fetch_func = partial(fetch_single_concert, api=api)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_code = {executor.submit(fetch_func, code): code for code in concert_codes}

        with tqdm(total=len(concert_codes), desc="내한공연 필터링") as pbar:
            for future in as_completed(future_to_code):
                valid_detail, excluded_detail, excluded_genre = future.result()
                if valid_detail:
                    valid_concerts.append(valid_detail)
                elif excluded_detail:
                    excluded_concerts.append(excluded_detail)
                    excluded_counts[excluded_genre] = excluded_counts.get(excluded_genre, 0) + 1
                pbar.update(1)

    return valid_concerts, excluded_concerts, excluded_counts


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
    excluded_concerts: List[Dict[str, Any]] = None,
    excluded_counts: Dict[str, int] = None,
):
    """비교 결과 출력"""
    excluded_concerts = excluded_concerts or []
    excluded_counts = excluded_counts or {}
    excluded_codes = {c.get('code', '') for c in excluded_concerts}
    new_codes = kopis_codes - db_codes
    # 키워드 필터에 걸린 공연은 KOPIS에 존재하는 것이므로 '사라진 공연'에서 제외
    removed_codes = db_codes - kopis_codes - excluded_codes

    total_excluded = sum(excluded_counts.values())
    total_kopis = len(kopis_codes) + total_excluded

    print("\n" + "=" * 80)
    print("🔍 KOPIS vs DB 비교 결과 (테스트용, 키워드 필터)")
    print("=" * 80)
    print(f"📊 통계:")
    print(f"   - KOPIS 내한 공연: {total_kopis}개")
    print(f"   - DB 현재/미래 공연: {len(db_codes)}개")
    for genre, count in sorted(excluded_counts.items()):
        if count > 0:
            print(f"   - {genre} 공연 (제외됨): {count}개")
    print(f"   - 새로 추가된 공연: {len(new_codes)}개")
    print(f"   - 사라진 공연: {len(removed_codes)}개")
    print("=" * 80)

    if not new_codes and not removed_codes and not excluded_concerts:
        print("\n✅ 변경 사항이 없습니다.")
        return

    if new_codes:
        print(f"\n{'=' * 80}")
        print(f"✨ 새로 추가된 공연 - {len(new_codes)}개")
        print(f"{'=' * 80}")
        for idx, code in enumerate(sorted(new_codes), 1):
            print_concert_info(idx, code, kopis_concerts.get(code, {}))

    if removed_codes:
        print(f"\n{'=' * 80}")
        print(f"🗑️  사라진 공연 - {len(removed_codes)}개")
        print(f"{'=' * 80}")
        for idx, code in enumerate(sorted(removed_codes), 1):
            print_concert_info(idx, code, db_concerts.get(code, {}))

    if excluded_concerts:
        print(f"\n{'=' * 80}")
        print(f"❌ 키워드 필터 제외 공연 - {len(excluded_concerts)}개")
        print(f"{'=' * 80}")
        for idx, concert in enumerate(sorted(excluded_concerts, key=lambda x: x.get('code', '')), 1):
            print_concert_info(idx, concert.get('code', ''), concert)

    print("\n" + "=" * 80)
    print("✅ 테스트 완료")
    print("=" * 80)


def compare_concerts_test():
    """테스트용 비교 함수"""
    kopis_api = KopisAPI(api_key=Config.KOPIS_API_KEY)
    db_manager = get_db_manager()

    try:
        logger.info("=" * 50)
        logger.info("🧪 공연 데이터 검수 시작 (테스트 모드)")
        logger.info("=" * 50)
        
        logger.info("데이터베이스에 연결하는 중...")
        if not db_manager.connect_with_ssh():
            logger.error("❌ 데이터베이스 연결에 실패했습니다.")
            return

        today = datetime.now()
        today_str = today.strftime("%Y%m%d")
        today_for_db = today.strftime("%Y.%m.%d")
        
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

        last_day = monthrange(max_db_date.year, max_db_date.month)[1]
        end_of_month = max_db_date.replace(day=last_day)
        end_date_str = end_of_month.strftime("%Y%m%d")
        max_db_date_str = end_of_month.strftime("%Y.%m.%d")
        
        logger.info(f"✅ 비교 기간: {today_for_db} ~ {max_db_date_str}")

        logger.info(f"\n📡 KOPIS API에서 공연 목록을 가져오는 중...")
        all_kopis_codes = kopis_api.fetch_all_concerts(start_date=today_str, end_date=end_date_str)
        logger.info(f"✅ KOPIS에서 총 {len(all_kopis_codes)}개의 공연을 찾았습니다.")
        
        if not all_kopis_codes:
            logger.warning("⚠️ KOPIS에서 가져온 공연이 없습니다.")
            return
        
        logger.info(f"\n🔍 내한 공연 필터링 중...")
        concert_details, excluded_concerts, excluded_counts = fetch_concerts_parallel(
            all_kopis_codes, api=kopis_api, max_workers=20
        )

        kopis_concerts = {detail['code']: detail for detail in concert_details}
        kopis_codes = set(kopis_concerts.keys())
        logger.info(f"✅ KOPIS에서 {len(kopis_codes)}개의 내한 공연을 찾았습니다. (키워드 제외: {len(excluded_concerts)}개)")

        logger.info(f"\n💾 데이터베이스에서 공연 목록을 가져오는 중...")
        db_concerts = get_db_concerts(db_manager, today_for_db, max_db_date_str)
        db_codes = set(db_concerts.keys())
        logger.info(f"✅ 데이터베이스에서 {len(db_codes)}개의 공연을 찾았습니다.")

        logger.info("\n🔄 공연 목록 비교 중...")
        print_comparison_results(kopis_codes, db_codes, kopis_concerts, db_concerts, excluded_concerts, excluded_counts)

    except Exception as e:
        logger.error(f"❌ 오류 발생: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        if db_manager:
            db_manager.disconnect()
            logger.info("\n🔌 데이터베이스 연결을 종료했습니다.")


if __name__ == "__main__":
    compare_concerts_test()