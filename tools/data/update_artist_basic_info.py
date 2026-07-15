import sys
import os
import pandas as pd
import logging
from tqdm import tqdm
import numpy as np
import argparse

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from lib.config import Config
from lib.data_collector import DataCollector
from lib.safe_writer import SafeWriter

# --- API Client Selection ---
if Config.USE_GEMINI_API:
    try:
        from core.apis.gemini_api import GeminiAPI as APIClient
    except ImportError:
        print("Warning: Gemini API not found, falling back to Perplexity.")
        from core.apis.perplexity_api import PerplexityAPI as APIClient
else:
    from core.apis.perplexity_api import PerplexityAPI as APIClient

# --- Logging Setup ---
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL, 'INFO'),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def fill_missing_artist_info(artist_name_to_update: str = None):
    """
    Loads artists.csv, finds artists with missing information,
    generates it using an AI API, and overwrites the file.
    """
    print("🚀 아티스트 기본 정보 채우기 스크립트 시작")

    try:
        # --- Initialization ---
        Config.set_test_mode(False)  # Ensure we are not in test mode
        Config.validate_api_keys()

        api_client = APIClient(Config.GEMINI_API_KEY if Config.USE_GEMINI_API else Config.PERPLEXITY_API_KEY)
        data_collector = DataCollector(api_client)
        
        artists_path = Config.OUTPUT_DIR / "artists.csv"
        concerts_path = Config.OUTPUT_DIR / "concerts.csv"

        # --- File Existence Check ---
        if not artists_path.exists():
            print(f"❌ 에러: {artists_path} 파일을 찾을 수 없습니다.")
            return
        if not concerts_path.exists():
            print(f"❌ 에러: {concerts_path} 파일을 찾을 수 없습니다.")
            return

        print(f"📊 {artists_path} 파일 로드 중...")
        df = pd.read_csv(artists_path)
        
        print(f"📊 {concerts_path} 파일 로드 중...")
        concerts_df = pd.read_csv(concerts_path)

        # --- Find rows to update ---
        # Ensure 'detail' column exists and fill NaN
        if 'detail' not in df.columns:
            df['detail'] = ''
        df['detail'] = df['detail'].fillna('')
        
        # Ensure 'musicbrainz_id' column exists and fill NaN
        if 'musicbrainz_id' not in df.columns:
            df['musicbrainz_id'] = ''
        df['musicbrainz_id'] = df['musicbrainz_id'].fillna('')
        
        if artist_name_to_update:
            print(f"🔍 특정 아티스트 '{artist_name_to_update}' 정보 업데이트를 시작합니다.")
            # Temporarily clear all relevant fields for the specified artist to force update
            fields_to_clear = ['category', 'detail', 'instagram_url', 'keywords', 'img_url', 'debut_date', 'nationality', 'group_type', 'musicbrainz_id']
            
            artist_mask = df['artist'] == artist_name_to_update
            
            for field in fields_to_clear:
                if field in df.columns:
                    # Use np.nan for potentially numeric columns to avoid dtype warnings
                    if field == 'debut_date':
                         df.loc[artist_mask, field] = np.nan
                    else:
                         df.loc[artist_mask, field] = ''
            
            to_update = df[df['artist'] == artist_name_to_update]
            if to_update.empty:
                print(f"❌ 에러: '{artist_name_to_update}' 아티스트를 찾을 수 없습니다. 정확한 이름을 확인해주세요.")
                return
        else:
            # Find artists where detail is empty
            to_update = df[df['detail'] == '']
            
            if to_update.empty:
                print("✅ 모든 아티스트에 기본 정보가 이미 존재합니다. 작업을 종료합니다.")
                return

            print(f"🔍 총 {len(df)}명 아티스트 중 {len(to_update)}명의 정보가 비어있습니다. 업데이트를 시작합니다.")

        # --- Update artist info ---
        updated_count = 0
        for index, row in tqdm(to_update.iterrows(), total=len(to_update), desc="- 아티스트 정보 생성 중"):
            try:
                artist_name = row['artist']

                if not artist_name or pd.isna(artist_name):
                    logger.warning(f"  - 건너뛰기: {index}번 행의 아티스트 이름이 없습니다.")
                    continue

                logger.info(f"작업 중: '{artist_name}'")
                
                # Find a concert by this artist to use as context
                artist_concerts = concerts_df[concerts_df['artist'] == artist_name]
                concert_title = artist_concerts['title'].iloc[0] if not artist_concerts.empty else None
                
                if concert_title:
                    logger.info(f"  - '{artist_name}' 아티스트의 콘서트 '{concert_title}'를 컨텍스트로 사용합니다.")

                new_info = data_collector._collect_artist_basic_info(artist_name, concert_title)

                if new_info:
                    # Update all fields from the returned dictionary
                    for key, value in new_info.items():
                        if key in df.columns:
                            df.at[index, key] = value
                    
                    logger.info(f"  - 성공: '{artist_name}'의 정보 생성 완료.")
                    updated_count += 1
                else:
                    logger.warning(f"  - 실패: '{artist_name}'의 정보 생성에 실패했습니다. API 응답이 비어있습니다.")

            except Exception as e:
                logger.error(f"  - 에러: '{row.get('artist', 'N/A')}' 처리 중 오류 발생: {e}", exc_info=True)
                continue
        
        # --- Save updated data ---
        if updated_count > 0:
            print(f"\n💾 {updated_count}명의 아티스트 정보를 업데이트하여 {artists_path} 파일에 덮어쓰는 중...")
            SafeWriter.save_dataframe(df, "artists.csv", backup_if_main=True)
            print("✅ 파일 저장이 완료되었습니다.")
        else:
            print("ℹ️ 업데이트된 항목이 없어 파일을 저장하지 않았습니다.")

        print(f"🎉 총 {updated_count}명의 아티스트 정보 업데이트 완료!")

    except FileNotFoundError:
        logger.error(f"오류: 설정된 경로에 파일이 없습니다. ({Config.OUTPUT_DIR / 'artists.csv'})")
    except Exception as e:
        logger.error(f"스크립트 실행 중 예상치 못한 오류가 발생했습니다: {e}", exc_info=True)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="아티스트 기본 정보를 채우거나 업데이트합니다.")
    parser.add_argument(
        "--artist", 
        type=str, 
        default=None,
        help="특정 아티스트의 정보를 업데이트합니다. (예: --artist '아이유')"
    )
    args = parser.parse_args()

    fill_missing_artist_info(artist_name_to_update=args.artist)