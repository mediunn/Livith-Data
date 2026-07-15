#!/usr/bin/env python3
"""
콘서트 데이터 수집 메인 파이프라인
"""
import sys
import os
import argparse
import logging
import traceback

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from lib.config import Config
from lib.db_utils import get_db_manager, get_dev_db_manager, get_stage_db_manager
from lib.discord_notifier import notify_kopis_done
from core.pipeline.data_pipeline import DataPipeline
from tools.database.upsert_csv_to_mysql import upsert_table
from tools.data.populate_schedules_from_concerts import populate_schedules
from tools.data.update_concert_genres import update_concert_genres

# 로깅 설정
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL, 'INFO'),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_date_input():
    """사용자로부터 날짜를 입력받는 함수"""
    start_date = input("수집 시작날짜를 적어주세요 (ex: 20250910): ")
    end_date = input("수집 마지막날짜를 적어주세요 (ex: 20251231): ")
    print(f"{start_date[:4]}년 {start_date[4:6]}월 {start_date[6:]}일부터 {end_date[:4]}년 {end_date[4:6]}월 {end_date[6:]}일까지의 데이터를 수집합니다.")
    return start_date, end_date


def parse_codes(codes_str: str) -> list:
    """콤마로 구분된 공연 코드 문자열을 리스트로 변환"""
    if not codes_str:
        return []
    return [code.strip() for code in codes_str.split(',') if code.strip()]


DB_FACTORIES = {
    'dev': get_dev_db_manager,
    'prod': get_db_manager,
    'stage': get_stage_db_manager,
}


def run_auto_pipeline(db_factory, pipeline, db_label: str = "dev"):
    """수집 후 upsert까지 전 과정 자동 실행"""
    print("\n[1/7] CSV 데이터 수집 완료 (main pipeline 실행됨)")

    print("\n[2/7] artists upsert 중...")
    if not upsert_table("artists", "artists.csv", db=db_factory()):
        print("❌ artists upsert 실패")
        return False

    print("\n[3/7] concerts upsert 중...")
    if not upsert_table("concerts", "concerts.csv", db=db_factory()):
        print("❌ concerts upsert 실패")
        return False

    print("\n[4/7] schedule.csv 생성 중 (KOPIS API)...")
    populate_schedules()

    print("\n[5/7] schedule upsert 중...")
    if not upsert_table("schedule", "schedule.csv", db=db_factory()):
        print("❌ schedule upsert 실패")
        return False

    print("\n[6/7] concert_genres.csv 생성 중...")
    update_concert_genres()

    print("\n[7/7] concert_genres upsert 중...")
    if not upsert_table("concert_genres", "concert_genres.csv", db=db_factory()):
        print("❌ concert_genres upsert 실패")
        return False

    print(f"\n[CSV 백업] 타임스탬프 폴더에 복사 중...")
    pipeline.copy_to_run_dir("schedule.csv", "concert_genres.csv")

    print("\n✅ 전체 자동 파이프라인 완료!")

    notify_kopis_done(
        webhook_url=Config.DISCORD_WEBHOOK_URL,
        db_label=db_label,
        concerts=pipeline.stats["concerts"],
        artists=pipeline.stats["artists"],
        concert_list=pipeline.stats["concert_list"],
        artist_list=pipeline.stats["artist_list"],
    )
    return True


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='콘서트 데이터 수집 파이프라인')

    parser.add_argument('--test', action='store_true', help='테스트 모드 (샘플 데이터만)')
    parser.add_argument('--stage', type=int, choices=[1, 2, 3, 4, 5], help='특정 스테이지만 실행')
    parser.add_argument('--codes', nargs='+', help='특정 공연 코드들 (ex: --codes PF12345 PF12346)')
    parser.add_argument('--auto', action='store_true', help='수집 후 DB upsert까지 자동 실행')
    parser.add_argument('--db', choices=['dev', 'prod', 'stage'], default='dev',
                        help='--auto 시 사용할 DB (기본: dev)')

    args = parser.parse_args()

    try:
        # 설정 초기화
        Config.set_test_mode(args.test)
        Config.validate_api_keys()

        # --codes 옵션이 있으면 해당 코드만 처리
        if args.codes:
            raw = ' '.join(args.codes)
            concert_codes = parse_codes(raw.replace(' ', ','))
            if not concert_codes:
                print("❌ 유효한 공연 코드가 없습니다.")
                sys.exit(1)

            print(f"🎯 지정된 공연 코드 {len(concert_codes)}개를 처리합니다:")
            for code in concert_codes:
                print(f"   - {code}")

            pipeline = DataPipeline(concert_codes=concert_codes)
        else:
            # 기존 방식: 날짜 범위 입력
            start_date, end_date = get_date_input()
            pipeline = DataPipeline(start_date=start_date, end_date=end_date)

        # 파이프라인 실행
        if args.stage:
            success = pipeline.run_stage(args.stage)
        else:
            success = pipeline.run_full_pipeline()

        if not success:
            print("❌ 파이프라인 실행 실패")
            sys.exit(1)

        print("🎉 데이터 수집 완료!")

        if args.auto:
            db_factory = DB_FACTORIES[args.db]
            print(f"\n--auto 모드: [{args.db}] DB에 upsert 시작")
            if not run_auto_pipeline(db_factory, pipeline, db_label=args.db):
                sys.exit(1)

    except KeyboardInterrupt:
        print("\n⚠️ 사용자가 중단했습니다.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"실행 오류: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()