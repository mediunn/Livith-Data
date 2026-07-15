"""
Instagram 크롤링 파이프라인 실행 스크립트
항상 DB에 저장하며 CSV 백업도 함께 생성합니다.

python tools/data/run_instagram_pipeline.py
python tools/data/run_instagram_pipeline.py --account livenationkorea
python tools/data/run_instagram_pipeline.py --prod
python tools/data/run_instagram_pipeline.py --stage
"""
import sys
import os
import argparse
import logging
import traceback

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from lib.config import Config
from lib.db_utils import get_db_manager, get_dev_db_manager, get_stage_db_manager
from lib.data_collector import DataCollector
from lib.discord_notifier import notify_instagram_done
from core.apis.gemini_api import GeminiAPI
from core.apis.instagram_api import InstagramAPI
from core.pipeline.instagram_pipeline import InstagramPipeline

logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL, 'INFO'),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def select_db():
    print("어느 DB를 사용할까요?")
    print("  1. Dev DB")
    print("  2. Livith DB (프로덕션)")
    while True:
        choice = input("선택 (1/2): ").strip()
        if choice in ("1", "2"):
            break
        print("1 또는 2를 입력해주세요.")
    use_dev = choice == "1"
    print(f"→ [{'개발' if use_dev else '프로덕션'}] DB 사용\n")
    return use_dev


def main():
    parser = argparse.ArgumentParser(description='Instagram 크롤링 → DB 저장 + CSV 백업 파이프라인')
    parser.add_argument('--account', type=str, help='특정 계정만 처리 (@ 없이)')
    parser.add_argument('--dev', action='store_true', help='Dev DB 사용 (선택 메뉴 스킵)')
    parser.add_argument('--prod', action='store_true', help='프로덕션 DB 사용 (선택 메뉴 스킵)')
    parser.add_argument('--stage', action='store_true', help='Stage DB 사용 (선택 메뉴 스킵)')
    parser.add_argument('--max-posts', type=int, default=12, help='계정당 최대 수집 게시물 수 (기본: 12)')
    parser.add_argument('--no-intro', action='store_true', help='공연 introduction Gemini 생성 건너뜀 (빠른 실행)')
    args = parser.parse_args()

    if args.dev:
        db = get_dev_db_manager()
        db_label = "dev"
        print("→ [개발] DB 사용\n")
    elif args.prod:
        db = get_db_manager()
        db_label = "prod"
        print("→ [프로덕션] DB 사용\n")
    elif args.stage:
        db = get_stage_db_manager()
        db_label = "stage"
        print("→ [스테이지] DB 사용\n")
    else:
        use_dev = select_db()
        db = get_dev_db_manager() if use_dev else get_db_manager()
        db_label = "dev" if use_dev else "prod"

    ig_username = Config.INSTAGRAM_USERNAME
    ig_password = Config.INSTAGRAM_PASSWORD
    if not ig_username or not ig_password:
        print("❌ INSTAGRAM_USERNAME / INSTAGRAM_PASSWORD 환경변수가 설정되지 않았습니다.")
        sys.exit(1)

    try:
        if not db.connect_with_ssh():
            print("❌ DB 연결 실패")
            sys.exit(1)

        gemini = GeminiAPI(Config.GEMINI_API_KEY)
        data_collector = DataCollector(gemini)
        instagram_api = InstagramAPI(ig_username, ig_password)

        pipeline = InstagramPipeline(
            db=db,
            instagram_api=instagram_api,
            gemini_api=gemini,
            data_collector=data_collector,
            max_posts=args.max_posts,
            generate_introduction=not args.no_intro,
        )

        if args.account:
            logger.info(f"단일 계정 처리: @{args.account}")
            pipeline._process_account(args.account, last_crawled_at=None)
            pipeline._save_preview_csvs()
        else:
            pipeline.run()

        print("\n완료!")
        concert_list = [
            f"{c['title']} — {c['artist']} ({c.get('start_date', '날짜미정')})  [@{c['code'].split('_insta_')[0]}]"
            for c in pipeline._preview_concerts
        ]
        artist_list = [a["artist"] for a in pipeline._preview_artists]
        notify_instagram_done(
            webhook_url=Config.DISCORD_WEBHOOK_URL,
            db_label=db_label,
            concerts=len(pipeline._preview_concerts),
            new_artists=len(pipeline._preview_artists),
            review_needed=len(pipeline._review_rows),
            concert_list=concert_list,
            artist_list=artist_list,
        )

    except KeyboardInterrupt:
        print("\n중단됨.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"실행 오류: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        db.disconnect()


if __name__ == "__main__":
    main()
