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

def update_concert_genres():
    """
    Loads concerts.csv, collects genre information for each concert,
    and saves the data to concert_genres.csv.
    """
    print("🚀 콘서트 장르 정보 수집 스크립트 시작")

    try:
        # --- Initialization ---
        Config.set_test_mode(False)
        Config.validate_api_keys()

        api_client = APIClient(Config.GEMINI_API_KEY if Config.USE_GEMINI_API else Config.PERPLEXITY_API_KEY)
        data_collector = DataCollector(api_client)
        
        concerts_path = Config.OUTPUT_DIR / "concerts.csv"
        genres_path = Config.OUTPUT_DIR / "concert_genres.csv"

        # --- File Existence Check ---
        if not concerts_path.exists():
            print(f"❌ 에러: {concerts_path} 파일을 찾을 수 없습니다.")
            return

        print(f"📊 {concerts_path} 파일 로드 중...")
        df = pd.read_csv(concerts_path, dtype={'id': 'Int64'})

        genre_rows = []
        print(f"🔍 총 {len(df)}개 콘서트의 장르 정보 수집을 시작합니다.")

        # --- Collect Genres ---
        for index, row in tqdm(df.iterrows(), total=len(df), desc="- 장르 정보 수집 중"):
            try:
                concert_id = row['id']
                title = row['title']
                artist = row['artist']

                if not all([concert_id, title, artist]) or pd.isna(artist):
                    logger.warning(f"  - 건너뛰기: {index}번 행의 ID, 제목, 또는 아티스트 정보가 부족합니다.")
                    continue
                
                genre_info_list = data_collector.collect_concert_genre(artist, title)

                if genre_info_list:
                    names = []
                    for genre_info in genre_info_list:
                        genre_info['concert_id'] = concert_id
                        genre_info['concert_title'] = title
                        genre_rows.append(genre_info)
                        names.append(genre_info.get('name', ''))
                    logger.info(f"  - 성공: '{title}'의 장르({', '.join(names)}) 수집 완료.")
                else:
                    logger.warning(f"  - 실패: '{title}'의 장르 수집에 실패했습니다.")

            except Exception as e:
                logger.error(f"  - 에러: '{row.get('title', 'N/A')}' 처리 중 오류 발생: {e}", exc_info=True)
                continue
        
        # --- Save updated data ---
        if genre_rows:
            print(f"\n💾 {len(genre_rows)}개의 장르 정보를 {genres_path} 파일에 저장하는 중...")
            genres_df = pd.DataFrame(genre_rows)
            
            # Ensure correct column order
            column_order = ['concert_id', 'concert_title', 'genre_id', 'name']
            genres_df = genres_df.reindex(columns=column_order)

            SafeWriter.save_dataframe(genres_df, "concert_genres.csv", backup_if_main=True)
            print("✅ 파일 저장이 완료되었습니다.")
        else:
            print("ℹ️ 수집된 장르 정보가 없어 파일을 저장하지 않았습니다.")

        print(f"🎉 총 {len(genre_rows)}개의 장르 정보 수집 완료!")

    except FileNotFoundError:
        logger.error(f"오류: 설정된 경로에 파일이 없습니다. ({concerts_path})")
    except Exception as e:
        logger.error(f"스크립트 실행 중 예상치 못한 오류가 발생했습니다: {e}", exc_info=True)

if __name__ == "__main__":
    update_concert_genres()
