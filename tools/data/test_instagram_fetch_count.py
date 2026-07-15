"""
계정별 수집 예정 게시물 수 + 스킵 이유 확인
python tools/data/test_instagram_fetch_count.py --dev
"""
import sys
import os
import argparse
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from lib.config import Config
from lib.db_utils import get_db_manager, get_dev_db_manager
from lib.prompts import CONCERT_KEYWORDS
from core.apis.instagram_api import InstagramAPI

parser = argparse.ArgumentParser()
parser.add_argument('--dev', action='store_true')
parser.add_argument('--prod', action='store_true')
args = parser.parse_args()

if args.dev:
    db = get_dev_db_manager()
elif args.prod:
    db = get_db_manager()
else:
    print("--dev 또는 --prod 옵션을 지정해주세요.")
    sys.exit(1)

if not db.connect_with_ssh():
    print("DB 연결 실패")
    sys.exit(1)

db.cursor.execute("SELECT account, last_crawled_at FROM crawl_history")
accounts = db.cursor.fetchall()
db.disconnect()

api = InstagramAPI(Config.INSTAGRAM_USERNAME, Config.INSTAGRAM_PASSWORD)

for account, last_crawled_at in accounts:
    print(f"\n{'='*50}")
    print(f"@{account} (last_crawled_at: {last_crawled_at})")

    resp = api.session.get(
        "https://i.instagram.com/api/v1/users/web_profile_info/",
        params={"username": account},
    )
    if resp.status_code != 200:
        print(f"  ❌ 프로필 조회 실패 ({resp.status_code}): {resp.text[:200]}")
        continue
    user = resp.json().get("data", {}).get("user")

    edges = user.get("edge_owner_to_timeline_media", {}).get("edges", [])
    print(f"  Instagram 반환: {len(edges)}개")

    since_utc = None
    if last_crawled_at:
        since_utc = last_crawled_at.replace(tzinfo=timezone.utc) if last_crawled_at.tzinfo is None else last_crawled_at

    for edge in edges:
        node = edge.get("node", {})
        ts = node.get("taken_at_timestamp", 0)
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        shortcode = node.get("shortcode", "")
        caption_edges = node.get("edge_media_to_caption", {}).get("edges", [])
        caption = caption_edges[0].get("node", {}).get("text", "") if caption_edges else ""
        caption_lower = caption.lower()
        has_keyword = any(kw in caption_lower for kw in CONCERT_KEYWORDS)

        if since_utc and dt <= since_utc:
            reason = "last_crawled_at 이전"
        elif not caption:
            reason = "캡션 없음"
        elif not has_keyword:
            reason = f"키워드 없음"
        else:
            reason = "✅ Gemini 처리 대상"

        print(f"  [{dt.strftime('%Y-%m-%d %H:%M')}] {shortcode} → {reason}")
