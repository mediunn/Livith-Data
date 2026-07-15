"""
representative_artists 테이블에서 아티스트를 가져와
한국어 이름을 추가하는 스크립트

DB데이터 가져와서 기본 한국어 이름 추가
python tools/data/add_represent_artists_korean.py

DB데이터 가져와서 한국어 없는 아티스트 일본어/특수문자 처리
python tools/data/add_represent_artists_korean.py --retry
"""
import sys
import os
import pandas as pd
import logging
from tqdm import tqdm

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from lib.config import Config
from lib.db_utils import get_db_manager, get_dev_db_manager
from lib.safe_writer import SafeWriter

# API Client Selection
if Config.USE_GEMINI_API:
    try:
        from core.apis.gemini_api import GeminiAPI as APIClient
    except ImportError:
        from core.apis.perplexity_api import PerplexityAPI as APIClient
else:
    from core.apis.perplexity_api import PerplexityAPI as APIClient

logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL, 'INFO'),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_representative_artists(use_dev: bool = False):
    """DB에서 representative_artists 테이블을 가져와 CSV로 저장"""
    db = get_dev_db_manager() if use_dev else get_db_manager()

    try:
        if not db.connect_with_ssh():
            print("❌ DB 연결 실패")
            return None

        print("📡 representative_artists 테이블 조회 중...")
        db.cursor.execute("SELECT * FROM representative_artists")
        columns = [desc[0] for desc in db.cursor.description]
        rows = db.cursor.fetchall()

        df = pd.DataFrame(rows, columns=columns)
        print(f"📊 {len(df)}명의 대표 아티스트 조회 완료")

        # CSV 저장
        SafeWriter.save_dataframe(df, "representative_artists.csv", backup_if_main=False)
        print(f"💾 representative_artists.csv 저장 완료")

        return df

    finally:
        db.disconnect()


def add_korean_names(df: pd.DataFrame, batch_size: int = 50) -> pd.DataFrame:
    """아티스트 이름에 한국어를 추가 (배치 처리)"""
    Config.validate_api_keys()
    api_client = APIClient(Config.GEMINI_API_KEY if Config.USE_GEMINI_API else Config.PERPLEXITY_API_KEY)

    # 처리 대상 필터링
    targets = []
    for index, row in df.iterrows():
        artist_name = row.get('artist_name', '')
        if not artist_name or pd.isna(artist_name):
            continue
        if '(' in artist_name and ')' in artist_name:
            continue
        if all('\uac00' <= c <= '\ud7a3' or c == ' ' for c in artist_name):
            continue
        targets.append((index, artist_name))

    skipped = len(df) - len(targets)
    print(f"📊 전체 {len(df)}명 중 {skipped}명 스킵, {len(targets)}명 처리 대상")

    # 배치 처리
    updated_count = 0
    total_batches = (len(targets) + batch_size - 1) // batch_size

    for batch_num in tqdm(range(total_batches), desc="배치 처리 중"):
        batch = targets[batch_num * batch_size : (batch_num + 1) * batch_size]
        artist_names = [name for _, name in batch]

        # 번호를 매겨서 키 매칭 문제 방지
        numbered_list = "\n".join([f"{i+1}. {name}" for i, name in enumerate(artist_names)])
        prompt = f"""다음 아티스트들의 한국어 표기를 알려줘.

{numbered_list}

📋 **필수 형식: "원어 (한국어)"**
- 원어: 입력된 이름 그대로 유지 (일본어, 특수문자 등 그대로)
- 한국어: 한국에서 통용되는 한국어 표기

✅ 올바른 예시:
- "Coldplay (콜드플레이)"
- "米津玄師 (요네즈 켄시)"
- "椎名林檎 (시이나 링고)"
- "Björk (비요크)"

반드시 JSON으로 응답. 키는 번호(문자열), 값은 "원어 (한국어)" 형식:
{{"1": "Lisa (리사)", "2": "米津玄師 (요네즈 켄시)", ...}}"""

        try:
            response = api_client.query_json(prompt, use_search=True)

            if not response:
                logger.warning(f"  ❌ 배치 {batch_num + 1}: API 응답 없음")
                continue

            for i, (index, artist_name) in enumerate(batch):
                new_name = response.get(str(i + 1), '')
                if new_name and '(' in new_name and ')' in new_name:
                    df.at[index, 'artist_name'] = new_name
                    print(f"  ✅ '{artist_name}' → '{new_name}'")
                    updated_count += 1
                elif new_name:
                    logger.warning(f"  ⚠️ '{artist_name}': 형식 불일치 '{new_name}'")
                else:
                    logger.warning(f"  ❌ '{artist_name}': 응답에 없음")

        except Exception as e:
            logger.error(f"  ❌ 배치 {batch_num + 1} 처리 중 오류: {e}")
            continue

    print(f"\n✅ {updated_count}명의 아티스트 한국어 이름 추가 완료")
    return df


def update_db(df: pd.DataFrame, use_dev: bool = False):
    """변경된 아티스트 이름을 DB에 업데이트"""
    db = get_dev_db_manager() if use_dev else get_db_manager()

    try:
        if not db.connect_with_ssh():
            print("❌ DB 연결 실패")
            return

        updated = 0
        for _, row in df.iterrows():
            artist_name = row.get('artist_name', '')
            artist_id = row.get('id', None)

            if not artist_id or not artist_name:
                continue

            try:
                db.cursor.execute(
                    "UPDATE representative_artists SET artist_name = %s WHERE id = %s",
                    (artist_name, int(artist_id))
                )
                updated += 1
            except Exception as e:
                logger.error(f"  DB 업데이트 실패 (id={artist_id}): {e}")

        db.commit()
        print(f"💾 DB 업데이트 완료: {updated}건")

    finally:
        db.disconnect()


def main():
    import argparse
    parser = argparse.ArgumentParser(description='대표 아티스트 한국어 이름 추가')
    parser.add_argument('--retry', action='store_true', help='CSV에서 한국어 없는 아티스트만 재처리')
    args = parser.parse_args()

    print("🚀 대표 아티스트 한국어 이름 추가 스크립트 시작\n")

    print("어느 DB를 사용할까요?")
    print("  1. Dev DB")
    print("  2. Livith DB (프로덕션)")
    while True:
        db_choice = input("선택 (1/2): ").strip()
        if db_choice in ("1", "2"):
            break
        print("1 또는 2를 입력해주세요.")
    use_dev = db_choice == "1"
    print(f"→ [{'개발' if use_dev else '프로덕션'}] DB 사용\n")

    if args.retry:
        # CSV에서 로드하여 실패한 것만 재처리
        csv_path = Config.OUTPUT_DIR / "representative_artists.csv"
        if not csv_path.exists():
            print("❌ representative_artists.csv 파일이 없습니다. --retry 없이 먼저 실행하세요.")
            return
        df = pd.read_csv(csv_path)
        print(f"📊 CSV에서 {len(df)}명 로드")
    else:
        df = fetch_representative_artists(use_dev=use_dev)
        if df is None or df.empty:
            print("❌ 데이터가 없습니다.")
            return

    # 한국어 이름 추가
    df = add_korean_names(df)

    # CSV 저장
    SafeWriter.save_dataframe(df, "representative_artists.csv", backup_if_main=False)
    print("💾 representative_artists.csv 업데이트 저장 완료")

    # DB 업데이트 여부 확인
    answer = input("\n📤 DB에도 업데이트하시겠습니까? (y/n): ").strip().lower()
    if answer == 'y':
        update_db(df, use_dev=use_dev)

    print("\n🎉 완료!")


if __name__ == "__main__":
    main()
