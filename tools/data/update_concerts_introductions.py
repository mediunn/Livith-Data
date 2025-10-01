import sys
import os
import pandas as pd
import logging
from tqdm import tqdm

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from lib.config import Config
from lib.data_collector import DataCollector
from lib.safe_writer import SafeWriter

# --- API Client Selection ---
# This logic is copied from data_pipeline.py to ensure consistency
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

def fill_missing_introductions():
    """
    Loads concerts.csv, finds concerts with missing introductions,
    generates them using an AI API, and overwrites the file.
    """
    print("🚀 한 줄 요약(introduction) 채우기 스크립트 시작")

    try:
        # --- Initialization ---
        Config.set_test_mode(False)  # Ensure we are not in test mode
        Config.validate_api_keys()

        api_client = APIClient(Config.GEMINI_API_KEY if Config.USE_GEMINI_API else Config.PERPLEXITY_API_KEY)
        data_collector = DataCollector(api_client)
        
        concerts_path = Config.OUTPUT_DIR / "concerts.csv"

        # --- File Existence Check ---
        if not concerts_path.exists():
            print(f"❌ 에러: {concerts_path} 파일을 찾을 수 없습니다.")
            return

        print(f"📊 {concerts_path} 파일 로드 중...")
        df = pd.read_csv(concerts_path)

        # --- Find rows to update ---
        # Ensure 'introduction' column exists
        if 'introduction' not in df.columns:
            df['introduction'] = ''
            
        # Fill NaN with empty strings to have a consistent type
        df['introduction'] = df['introduction'].fillna('')
        
        to_update = df[df['introduction'] == '']
        
        if to_update.empty:
            print("✅ 모든 콘서트에 한 줄 요약이 이미 존재합니다. 작업을 종료합니다.")
            return

        print(f"🔍 총 {len(df)}개 콘서트 중 {len(to_update)}개의 한 줄 요약이 비어있습니다. 업데이트를 시작합니다.")

        # --- Update introductions ---
        updated_count = 0
        # Use tqdm for a progress bar
        for index, row in tqdm(to_update.iterrows(), total=len(to_update), desc="- 한 줄 요약 생성 중"):
            try:
                title = row['title']
                artist = row['artist']

                if not title or not artist or pd.isna(title) or pd.isna(artist):
                    logger.warning(f"  - 건너뛰기: {index}번 행의 제목 또는 아티스트 정보가 부족합니다.")
                    continue

                # Use logger for consistent output, but print for immediate feedback on current item
                logger.info(f"작업 중: '{title}' ({artist})")
                
                new_introduction = data_collector._collect_short_introduction(title, artist)

                if new_introduction:
                    df.at[index, 'introduction'] = new_introduction
                    logger.info(f"  - 성공: '{title}'의 한 줄 요약 생성 완료.")
                    updated_count += 1
                else:
                    logger.warning(f"  - 실패: '{title}'의 한 줄 요약 생성에 실패했습니다. API 응답이 비어있습니다.")

            except Exception as e:
                logger.error(f"  - 에러: '{row.get('title', 'N/A')}' 처리 중 오류 발생: {e}", exc_info=True)
                continue
        
        # --- Save updated data ---
        if updated_count > 0:
            print(f"\n💾 {updated_count}개의 한 줄 요약을 업데이트하여 {concerts_path} 파일에 덮어쓰는 중...")
            # Assuming SafeWriter.save_dataframe is the correct method from the project structure
            SafeWriter.save_dataframe(df, "concerts.csv", backup_if_main=True)
            print("✅ 파일 저장이 완료되었습니다.")
        else:
            print("ℹ️ 업데이트된 항목이 없어 파일을 저장하지 않았습니다.")

        print(f"🎉 총 {updated_count}개의 콘서트 정보 업데이트 완료!")

    except FileNotFoundError:
        logger.error(f"오류: 설정된 경로에 파일이 없습니다. ({Config.OUTPUT_DIR / 'concerts.csv'})")
    except Exception as e:
        logger.error(f"스크립트 실행 중 예상치 못한 오류가 발생했습니다: {e}", exc_info=True)

if __name__ == "__main__":
    fill_missing_introductions()
