#!/usr/bin/env python3
"""
artists.csv의 twitter_url 컬럼을 MusicBrainz에서 수집하여 채우는 스크립트
"""
import sys
import os
import time
import argparse
import pandas as pd
from tqdm import tqdm
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.apis.musicbrainz_api import MusicBrainzAPI
from lib.config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def fetch_twitter_url(mb_api: MusicBrainzAPI, row: pd.Series) -> str:
    artist_name = row.get('artist', '')
    mbid = row.get('musicbrainz_id', '')

    if mbid and str(mbid).strip():
        mb_details = mb_api.get_artist_by_id(str(mbid).strip())
    else:
        import re
        search_name = re.sub(r'\s*\([^)]+\)', '', artist_name).strip()
        results = mb_api.search_artist(search_name, limit=3)
        if not results:
            return ''
        best = max(results, key=lambda x: int(x.get('score') or 0), default=None)
        if not best:
            return ''
        mb_details = mb_api.get_artist_by_id(best['id'])
        time.sleep(1)

    return mb_api.extract_twitter_url(mb_details)


def main():
    parser = argparse.ArgumentParser(description="artists.csv의 twitter_url을 MusicBrainz에서 수집합니다.")
    parser.add_argument('--artist', type=str, default=None, help="특정 아티스트만 업데이트 (예: --artist '아이유')")
    parser.add_argument('--overwrite', action='store_true', help="이미 twitter_url이 있어도 덮어씀")
    args = parser.parse_args()

    artists_path = Config.OUTPUT_DIR / 'artists.csv'
    df = pd.read_csv(artists_path, encoding='utf-8-sig', dtype={'id': 'Int64'})

    if 'twitter_url' not in df.columns:
        df['twitter_url'] = ''
    df['twitter_url'] = df['twitter_url'].fillna('')

    if args.artist:
        mask = df['artist'] == args.artist
        if mask.sum() == 0:
            print(f"❌ '{args.artist}' 아티스트를 찾을 수 없습니다.")
            return
        targets = df[mask]
    elif args.overwrite:
        targets = df
    else:
        targets = df[df['twitter_url'] == '']

    print(f"🔍 {len(targets)}명 아티스트의 Twitter URL 수집 시작")

    mb_api = MusicBrainzAPI()
    updated = 0

    for index, row in tqdm(targets.iterrows(), total=len(targets), desc="Twitter URL 수집 중"):
        artist_name = row.get('artist', '')
        try:
            twitter_url = fetch_twitter_url(mb_api, row)
            if twitter_url:
                df.at[index, 'twitter_url'] = twitter_url
                logger.info(f"  ✅ '{artist_name}': {twitter_url}")
                updated += 1
            else:
                logger.info(f"  - '{artist_name}': MusicBrainz에 Twitter URL 없음")
            time.sleep(1)
        except Exception as e:
            logger.error(f"  ❌ '{artist_name}' 처리 중 오류: {e}")

    if updated > 0:
        df.to_csv(artists_path, index=False, encoding='utf-8-sig')
        print(f"\n✅ {updated}명 업데이트 완료 → {artists_path}")
    else:
        print("\nℹ️ 업데이트된 항목 없음")


if __name__ == '__main__':
    main()
