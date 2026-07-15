import sys
import os
import logging

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from lib.config import Config
from lib.data_collector import DataCollector
from lib.data_models import Artist

# --- Logging Setup ---
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL, 'INFO'),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_musicbrainz_integration():
    print("🚀 MusicBrainz 통합 테스트 시작")

    try:
        # --- Initialization ---
        Config.set_test_mode(True) # Use test mode for API calls if applicable
        Config.validate_api_keys()

        # Select API client based on configuration
        if Config.USE_GEMINI_API:
            try:
                from core.apis.gemini_api import GeminiAPI as APIClient
            except ImportError:
                logger.warning("Gemini API not found, falling back to Perplexity.")
                from core.apis.perplexity_api import PerplexityAPI as APIClient
        else:
            from core.apis.perplexity_api import PerplexityAPI as APIClient

        api_client = APIClient(Config.GEMINI_API_KEY if Config.USE_GEMINI_API else Config.PERPLEXITY_API_KEY)
        data_collector = DataCollector(api_client)

        # --- Test Case 1: Known Artist (Lauv) ---
        artist_name_1 = "Lauv"
        print(f"\n--- 아티스트 '{artist_name_1}' 정보 수집 테스트 ---")
        artist_info_1 = data_collector.collect_artist_info(artist_name_1)

        if artist_info_1:
            print(f"✅ '{artist_name_1}' 정보 수집 성공:")
            print(f"  Artist Name: {artist_info_1.artist}")
            print(f"  MusicBrainz ID: {artist_info_1.musicbrainz_id}")
            print(f"  Debut Date: {artist_info_1.debut_date}")
            print(f"  Nationality: {artist_info_1.nationality}")
            print(f"  Group Type: {artist_info_1.group_type}")
            print(f"  Instagram URL: {artist_info_1.instagram_url}")
            print(f"  Introduction: {artist_info_1.detail[:100]}...")
            assert artist_info_1.musicbrainz_id != "", "MusicBrainz ID should not be empty for Lauv"
            assert artist_info_1.artist == "Lauv", "Artist name should match"
        else:
            print(f"❌ '{artist_name_1}' 정보 수집 실패.")
            assert False, f"Failed to collect info for {artist_name_1}"

        # --- Test Case 2: Another Known Artist (BTS) ---
        artist_name_2 = "BTS"
        print(f"\n--- 아티스트 '{artist_name_2}' 정보 수집 테스트 ---")
        artist_info_2 = data_collector.collect_artist_info(artist_name_2)

        if artist_info_2:
            print(f"✅ '{artist_name_2}' 정보 수집 성공:")
            print(f"  Artist Name: {artist_info_2.artist}")
            print(f"  MusicBrainz ID: {artist_info_2.musicbrainz_id}")
            print(f"  Debut Date: {artist_info_2.debut_date}")
            print(f"  Nationality: {artist_info_2.nationality}")
            print(f"  Group Type: {artist_info_2.group_type}")
            print(f"  Instagram URL: {artist_info_2.instagram_url}")
            print(f"  Introduction: {artist_info_2.detail[:100]}...")
            assert artist_info_2.musicbrainz_id != "", "MusicBrainz ID should not be empty for BTS"
            assert artist_info_2.artist == "BTS", "Artist name should match"
        else:
            print(f"❌ '{artist_name_2}' 정보 수집 실패.")
            assert False, f"Failed to collect info for {artist_name_2}"

        print("\n🎉 MusicBrainz 통합 테스트 완료!")

    except Exception as e:
        logger.error(f"MusicBrainz 통합 테스트 중 오류 발생: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    test_musicbrainz_integration()
