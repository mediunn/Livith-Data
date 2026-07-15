"""
인스타그램 포스터 URL 갱신 스크립트

DB의 concerts 중 code가 '_insta_'를 포함한 것들의 포스터 URL을
shortcode로 다시 조회해서 갱신합니다.

python tools/data/refresh_instagram_posters.py --stage
python tools/data/refresh_instagram_posters.py --prod
python tools/data/refresh_instagram_posters.py --dev
python tools/data/refresh_instagram_posters.py --stage --all   # 포스터 있는 것도 강제 갱신
"""
import sys
import os
import argparse
import logging
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from lib.config import Config
from lib.db_utils import get_db_manager, get_dev_db_manager, get_stage_db_manager
from core.apis.instagram_api import InstagramAPI

logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL, 'INFO'),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='인스타그램 포스터 URL 갱신')
    parser.add_argument('--dev', action='store_true', help='Dev DB 사용')
    parser.add_argument('--prod', action='store_true', help='프로덕션 DB 사용')
    parser.add_argument('--stage', action='store_true', help='Stage DB 사용')
    parser.add_argument('--all', action='store_true', dest='force_all',
                        help='포스터가 있는 것도 강제 갱신 (기본: 비어있는 것만)')
    args = parser.parse_args()

    if args.dev:
        db = get_dev_db_manager()
        print("→ [개발] DB 사용")
    elif args.prod:
        db = get_db_manager()
        print("→ [프로덕션] DB 사용")
    elif args.stage:
        db = get_stage_db_manager()
        print("→ [스테이지] DB 사용")
    else:
        print("DB를 선택하세요: --dev / --stage / --prod")
        sys.exit(1)

    ig_username = Config.INSTAGRAM_USERNAME
    ig_password = Config.INSTAGRAM_PASSWORD
    if not ig_username or not ig_password:
        print("❌ INSTAGRAM_USERNAME / INSTAGRAM_PASSWORD 환경변수가 설정되지 않았습니다.")
        sys.exit(1)

    if not db.connect_with_ssh():
        print("❌ DB 연결 실패")
        sys.exit(1)

    try:
        if args.force_all:
            db.cursor.execute(
                "SELECT id, code, poster FROM concerts WHERE code LIKE %s",
                ('%_insta_%',)
            )
        else:
            db.cursor.execute(
                "SELECT id, code, poster FROM concerts "
                "WHERE code LIKE %s AND (poster IS NULL OR poster = '')",
                ('%_insta_%',)
            )

        rows = db.cursor.fetchall()
        if not rows:
            print("갱신할 포스터가 없습니다.")
            return

        print(f"갱신 대상: {len(rows)}개\n")

        instagram_api = InstagramAPI(ig_username, ig_password)

        updated, failed = 0, 0
        for concert_id, code, current_poster in rows:
            # code 형식: {account}_insta_{shortcode}
            parts = code.split('_insta_', 1)
            if len(parts) != 2:
                logger.warning(f"code 형식 오류, 스킵: {code}")
                continue

            account, shortcode = parts[0], parts[1]
            logger.info(f"[{concert_id}] {code} → @{account} shortcode={shortcode}")

            new_url = instagram_api.fetch_post_image_url(account, shortcode)
            if new_url:
                db.cursor.execute(
                    "UPDATE concerts SET poster = %s WHERE id = %s",
                    (new_url, concert_id)
                )
                db.commit()
                logger.info(f"  ✅ 갱신 완료")
                updated += 1
            else:
                logger.warning(f"  ⚠️ URL 조회 실패")
                failed += 1

            time.sleep(1)

        print(f"\n완료 — 성공: {updated}개 / 실패: {failed}개")

    except KeyboardInterrupt:
        print("\n중단됨.")
    finally:
        db.disconnect()


if __name__ == "__main__":
    main()
