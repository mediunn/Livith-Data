#!/usr/bin/env python3
"""
concerts.csv의 venue 컬럼을 KOPIS API에서 다시 가져와 중복 제거 후 덮어쓰는 스크립트
Gemini 호출 없음 - KOPIS API만 사용
"""
import sys
import pandas as pd
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from lib.config import Config
from lib.safe_writer import SafeWriter
from core.apis.kopis_api import KopisAPI


def fix_venue_names():
    concerts_path = Config.OUTPUT_DIR / "concerts.csv"

    if not concerts_path.exists():
        print(f"❌ concerts.csv 없음: {concerts_path}")
        return

    df = pd.read_csv(concerts_path, encoding='utf-8')
    kopis = KopisAPI(Config.KOPIS_API_KEY)

    # code가 있는 공연만 처리 (Instagram 출처 공연은 KOPIS 코드 없음)
    mask = df['code'].notna() & (df['code'] != '')
    targets = df[mask]

    print(f"총 {len(targets)}개 공연의 장소명 업데이트 시작...")

    updated = 0
    for idx, row in targets.iterrows():
        code = row['code']
        detail = kopis.get_concert_detail(code)
        if not detail:
            print(f"  ⚠️ KOPIS 조회 실패: {code}")
            continue

        new_venue = detail.get('venue', '')
        old_venue = row.get('venue', '')

        if new_venue and new_venue != old_venue:
            print(f"  ✅ {code}: '{old_venue}' → '{new_venue}'")
            df.at[idx, 'venue'] = new_venue
            updated += 1
        else:
            print(f"  - {code}: 변경 없음 ({old_venue})")

    if updated > 0:
        SafeWriter.save_dataframe(df, "concerts.csv", backup_if_main=True)
        print(f"\n✅ {updated}개 장소명 업데이트 완료!")
    else:
        print("\n변경된 장소명 없음.")


if __name__ == "__main__":
    fix_venue_names()
