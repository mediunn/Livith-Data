#!/usr/bin/env python3
"""
KOPIS API와 로컬 데이터베이스의 콘서트 목록을 비교
키워드 필터만 사용 (AI 필터링 없음)
- 제외된 공연 목록도 출력 및 Discord 전송
- 필터에 걸린 공연은 '사라진 공연'으로 오분류하지 않음
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

from core.apis.kopis_api import KopisAPI, KopisAPIError
from lib.db_utils import get_db_manager
from lib.config import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RateLimiter:
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

EXCLUDED_GENRES = [
    '재즈', '클래식',
    '연주회', '독주회',
    '기타리스트', '핑거스타일',
    '앙상블', '챔버', '콰르텟', '트리오',
    '관현악', '교향',
    '피아노',
]

_API_ERROR = '__api_error__'


def validate_config():
    required = ['KOPIS_API_KEY']
    missing = [key for key in required if not getattr(Config, key, None)]
    if missing:
        raise ValueError(f"필수 설정 누락: {missing}")


def is_visit_concert(detail: Dict[str, Any]) -> tuple[bool, str]:
    """
    내한공연 여부 확인
    Returns: (is_valid, excluded_genre)
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
    if date_str and len(date_str) == 8 and date_str.isdigit():
        return f"{date_str[:4]}.{date_str[4:6]}.{date_str[6:8]}"
    return date_str


def fetch_single_concert(code: str, api: KopisAPI, max_retries: int = 3) -> tuple[
    Optional[Dict[str, Any]],  # 필터 통과한 공연
    Optional[Dict[str, Any]],  # 키워드로 제외된 공연
    str                        # 제외 장르 or _API_ERROR
]:
    """
    단일 공연 정보를 가져오는 함수
    Returns: (valid_detail, excluded_detail, excluded_genre)
    - excluded_genre == _API_ERROR: 400 에러로 조회 실패
    """
    for attempt in range(max_retries):
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
                    # 키워드 필터에 걸림: 존재는 하지만 제외
                    return None, detail, excluded_genre
                else:
                    # visit != 'Y' 등 내한공연 아님
                    return None, None, ''
            if attempt < max_retries - 1:
                time.sleep(1 * (attempt + 1))
                continue
            return None, None, ''
        except KopisAPIError:
            return None, None, _API_ERROR
        except Exception as e:
            logger.warning(f"공연 코드 {code} 처리 실패 (시도 {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(1 * (attempt + 1))
                continue
            return None, None, ''
    return None, None, ''


def fetch_concerts_parallel(
    concert_codes: List[str],
    api: KopisAPI,
    max_workers: int = 10
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int], set]:
    """
    병렬 처리로 공연 정보를 가져오는 함수
    Returns: (valid_concerts, excluded_concerts, excluded_counts, api_error_codes)
    - valid_concerts: 필터 통과한 공연
    - excluded_concerts: 키워드 필터에 걸린 공연 (존재는 함)
    - excluded_counts: 장르별 제외 카운트
    - api_error_codes: 400 에러로 조회 실패한 코드
    """
    valid_concerts = []
    excluded_concerts = []
    excluded_counts: Dict[str, int] = {genre: 0 for genre in EXCLUDED_GENRES}
    api_error_codes: set = set()
    fetch_func = partial(fetch_single_concert, api=api)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_code = {
            executor.submit(fetch_func, code): code
            for code in concert_codes
        }

        with tqdm(total=len(concert_codes), desc="내한공연 필터링") as pbar:
            for future in as_completed(future_to_code):
                code = future_to_code[future]
                valid_detail, excluded_detail, excluded_genre = future.result()
                if valid_detail:
                    valid_concerts.append(valid_detail)
                elif excluded_detail:
                    excluded_concerts.append(excluded_detail)
                    excluded_counts[excluded_genre] = excluded_counts.get(excluded_genre, 0) + 1
                elif excluded_genre == _API_ERROR:
                    api_error_codes.add(code)
                pbar.update(1)

    # 400 에러 코드 재시도
    if api_error_codes:
        logger.info(f"⏳ 400 에러 {len(api_error_codes)}개 재시도 중...")
        time.sleep(2)
        still_error = set()
        for code in tqdm(api_error_codes, desc="400 에러 재시도"):
            rate_limiter.wait()
            try:
                detail = api.get_concert_detail(code)
                if detail:
                    is_valid, excluded_genre = is_visit_concert(detail)
                    detail['start_date'] = normalize_date(detail.get('start_date', ''))
                    detail['end_date'] = normalize_date(detail.get('end_date', ''))
                    if is_valid:
                        valid_concerts.append(detail)
                    elif excluded_genre:
                        excluded_concerts.append(detail)
                        excluded_counts[excluded_genre] = excluded_counts.get(excluded_genre, 0) + 1
                else:
                    still_error.add(code)
            except KopisAPIError:
                still_error.add(code)
        logger.info(f"✅ 재시도 결과: {len(api_error_codes) - len(still_error)}개 복구, {len(still_error)}개 여전히 실패")
        api_error_codes = still_error

    return valid_concerts, excluded_concerts, excluded_counts, api_error_codes


def get_db_concerts(db_manager, start_date: str, end_date: str) -> Dict[str, Dict[str, Any]]:
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


def print_comparison_results(
    kopis_codes: set,
    db_codes: set,
    kopis_concerts: Dict,
    db_concerts: Dict,
    excluded_concerts: List[Dict[str, Any]],
    excluded_counts: Dict[str, int],
):
    new_codes = kopis_codes - db_codes
    removed_codes = db_codes - kopis_codes

    total_excluded = sum(excluded_counts.values())
    total_kopis = len(kopis_codes) + total_excluded

    print("\n" + "=" * 80)
    print("🔍 KOPIS vs DB 비교 결과 (내한 공연 기준, 키워드 필터)")
    print("=" * 80)
    print(f"📊 통계:")
    print(f"   - KOPIS 내한 공연: {total_kopis}개")
    print(f"   - DB 현재/미래 공연: {len(db_codes)}개")
    print(f"   - 필터링된 공연: {total_excluded}개")
    print(f"   - 새로 추가된 공연: {len(new_codes)}개")
    print(f"   - 사라진 공연: {len(removed_codes)}개")
    print("=" * 80)

    if not new_codes and not removed_codes and not excluded_concerts:
        print("\n✅ 변경 사항이 없습니다.")
        return

    # 새로 추가된 공연
    if new_codes:
        print(f"\n{'=' * 80}")
        print(f"✨ 새로 추가된 공연 ({len(new_codes)}개)")
        print(f"{'=' * 80}")
        for idx, code in enumerate(sorted(new_codes), 1):
            details = kopis_concerts.get(code, {})
            print(f"\n{idx}. [{code}] {details.get('title', '제목 없음')}")
            print(f"   {details.get('start_date', 'N/A')} ~ {details.get('end_date', 'N/A')}")

    # 사라진 공연
    if removed_codes:
        print(f"\n{'=' * 80}")
        print(f"🗑️  사라진 공연 ({len(removed_codes)}개)")
        print(f"{'=' * 80}")
        print("⚠️  공연 취소 또는 KOPIS에서 삭제된 공연")
        for idx, code in enumerate(sorted(removed_codes), 1):
            details = db_concerts.get(code, {})
            print(f"\n{idx}. [{code}] {details.get('title', '제목 없음')}")
            print(f"   {details.get('start_date', 'N/A')} ~ {details.get('end_date', 'N/A')}")

    # 키워드 필터로 제외된 공연
    if excluded_concerts:
        print(f"\n{'=' * 80}")
        print(f"❌ 키워드 필터 제외 공연(연주곡) ({len(excluded_concerts)}개)")
        print(f"{'=' * 80}")
        for idx, concert in enumerate(sorted(excluded_concerts, key=lambda x: x.get('code', '')), 1):
            print(f"\n{idx}. [{concert.get('code')}] {concert.get('title', '제목 없음')}")
            print(f"   {concert.get('start_date', 'N/A')} ~ {concert.get('end_date', 'N/A')}")

    print("\n" + "=" * 80)
    print("✅ 비교 작업 완료")
    print("=" * 80)


def send_discord_results(
    notifier: DiscordNotifier,
    kopis_codes: set,
    db_codes: set,
    kopis_concerts: Dict,
    db_concerts: Dict,
    excluded_concerts: List[Dict[str, Any]],
    excluded_counts: Dict[str, int],
    start_date: str,
    end_date: str,
):
    new_codes = kopis_codes - db_codes
    removed_codes = db_codes - kopis_codes

    if not new_codes and not removed_codes and not excluded_concerts:
        logger.info("변경 사항 없음 - Discord 알림 스킵")
        return True

    today = datetime.now().strftime("%Y.%m.%d")
    total_excluded = sum(excluded_counts.values())
    total_kopis = len(kopis_codes) + total_excluded

    messages = []

    # 헤더 + 통계
    header = f"🎵 KOPIS 동기화 알림 ({today})\n"
    header += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    header += f"📆 비교 기간: {start_date} ~ {end_date}\n\n"
    header += "📊 통계\n"
    header += f"- KOPIS 내한 공연: {total_kopis}개\n"
    header += f"- DB 공연: {len(db_codes)}개\n"
    header += f"- 새로 추가: {len(new_codes)}개\n"
    header += f"- 사라진 공연: {len(removed_codes)}개\n"
    header += f"- 필터링된 공연: {total_excluded}개"
    messages.append(header)

    # 새로 추가된 공연
    if new_codes:
        msg = "\n━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"✨ 새로 추가된 공연 ({len(new_codes)}개)\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
        for idx, code in enumerate(sorted(new_codes), 1):
            details = kopis_concerts.get(code, {})
            entry = f"\n{idx}. [{code}] {details.get('title', '제목 없음')}\n"
            entry += f"　　{details.get('start_date', 'N/A')} ~ {details.get('end_date', 'N/A')}\n"
            if len(msg + entry) > 1900:
                messages.append(msg)
                msg = entry
            else:
                msg += entry
        messages.append(msg)

    # 사라진 공연
    if removed_codes:
        msg = "\n━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"🗑️ 사라진 공연 ({len(removed_codes)}개)\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += "⚠️ 공연 취소 또는 KOPIS에서 삭제된 공연\n"
        for idx, code in enumerate(sorted(removed_codes), 1):
            details = db_concerts.get(code, {})
            entry = f"\n{idx}. [{code}] {details.get('title', '제목 없음')}\n"
            entry += f"　　{details.get('start_date', 'N/A')} ~ {details.get('end_date', 'N/A')}\n"
            if len(msg + entry) > 1900:
                messages.append(msg)
                msg = entry
            else:
                msg += entry
        messages.append(msg)

    # 키워드 필터 제외 공연
    if excluded_concerts:
        msg = "\n━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"❌ 키워드 필터 제외 공연(연주곡) ({len(excluded_concerts)}개)\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
        for idx, concert in enumerate(sorted(excluded_concerts, key=lambda x: x.get('code', '')), 1):
            entry = f"\n{idx}. [{concert.get('code')}] {concert.get('title', '제목 없음')}\n"
            entry += f"　　{concert.get('start_date', 'N/A')} ~ {concert.get('end_date', 'N/A')}\n"
            if len(msg + entry) > 1900:
                messages.append(msg)
                msg = entry
            else:
                msg += entry
        messages.append(msg)

    success = True
    for msg in messages:
        if not notifier.send_message(msg.strip()):
            success = False
    return success


def compare_concerts() -> dict:
    validate_config()
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
        logger.info("🎵 공연 데이터 동기화 검사 시작 (키워드 필터)")
        logger.info("=" * 50)

        if not db_manager.connect_with_ssh():
            logger.error("❌ 데이터베이스 연결에 실패했습니다.")
            return result

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

        logger.info(f"\n🔍 내한 공연 필터링 중...")
        try:
            valid_concerts, excluded_concerts, excluded_counts, api_error_codes = fetch_concerts_parallel(
                all_kopis_codes,
                api=kopis_api,
                max_workers=20
            )
            if api_error_codes:
                logger.info(f"⚠️ API 400 에러로 조회 실패: {len(api_error_codes)}개 코드 (사라진 공연 판단 제외)")
            logger.info(f"✅ KOPIS에서 {len(valid_concerts)}개의 내한 공연을 찾았습니다. (키워드 제외: {len(excluded_concerts)}개)")
        except Exception as e:
            logger.error(f"❌ 공연 상세 정보 가져오기 중 오류 발생: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return result

        kopis_concerts = {c['code']: c for c in valid_concerts}
        kopis_codes = set(kopis_concerts.keys())

        # 키워드 필터에 걸린 공연도 KOPIS에 존재하는 것으로 취급 (사라진 공연 오분류 방지)
        all_existing_kopis_codes = kopis_codes | {c['code'] for c in excluded_concerts}

        logger.info(f"\n💾 데이터베이스에서 공연 목록을 가져오는 중...")
        try:
            db_concerts = get_db_concerts(db_manager, today_for_db, max_db_date_str)
            db_codes = set(db_concerts.keys()) - api_error_codes
            logger.info(f"✅ 데이터베이스에서 {len(db_codes)}개의 공연을 찾았습니다.")
        except Exception as e:
            logger.error(f"❌ 데이터베이스 조회 중 오류 발생: {e}")
            return result

        # 사라진 공연: DB에 있지만 KOPIS에 존재하지 않는 것 (필터 여부 무관)
        removed_codes = db_codes - all_existing_kopis_codes
        # 새 공연: KOPIS 필터 통과 + DB에 없는 것
        new_codes = kopis_codes - db_codes

        logger.info("\n🔄 공연 목록 비교 중...")
        print_comparison_results(
            kopis_codes, db_codes, kopis_concerts, db_concerts,
            excluded_concerts, excluded_counts
        )

        result['new_count'] = len(new_codes)
        result['removed_count'] = len(removed_codes)
        result['success'] = True

        if Config.DISCORD_WEBHOOK_URL:
            logger.info("📤 Discord 알림 전송 중...")
            notifier = DiscordNotifier(Config.DISCORD_WEBHOOK_URL)
            for attempt in range(3):
                if send_discord_results(
                    notifier,
                    kopis_codes, db_codes, kopis_concerts, db_concerts,
                    excluded_concerts, excluded_counts,
                    start_date=today_for_db,
                    end_date=max_db_date_str
                ):
                    logger.info("✅ Discord 알림 전송 완료")
                    break
                logger.warning(f"⚠️ Discord 알림 전송 실패 (시도 {attempt + 1}/3)")
                time.sleep(2)

    except Exception as e:
        logger.error(f"❌ 비교 작업 중 예상치 못한 오류 발생: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        if db_manager:
            db_manager.disconnect()
            logger.info("\n🔌 데이터베이스 연결을 종료했습니다.")
        elapsed = time.time() - start_time
        result['elapsed_time'] = elapsed
        logger.info(f"⏱️ 총 소요 시간: {elapsed:.1f}초")

    return result


if __name__ == "__main__":
    compare_concerts()
