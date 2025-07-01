import sys
import os
import logging
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config
from src.perplexity_api import PerplexityAPI
from src.data_collector import ConcertDataCollector
from src.csv_manager import CSVDataManager

logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    try:
        Config.validate()
        
        perplexity_api = PerplexityAPI(Config.PERPLEXITY_API_KEY)
        collector = ConcertDataCollector(perplexity_api)
        
        artist_name = input("아티스트 이름을 입력하세요: ").strip()
        
        if not artist_name:
            print("아티스트 이름이 입력되지 않았습니다.")
            return
        
        print(f"\n{artist_name}의 데이터를 수집중...")
        
        # 1. 콘서트 정보 수집
        print("1. 콘서트 정보 수집 중...")
        concerts = collector.get_artist_concerts(artist_name)
        
        if not concerts:
            print("콘서트 정보를 찾을 수 없습니다.")
            return
        
        print(f"   {len(concerts)}개의 콘서트 정보를 수집했습니다.")
        
        # 2. 각 콘서트의 셋리스트 정보 수집
        print("2. 셋리스트 정보 수집 중...")
        all_setlists = []
        all_setlist_songs = []
        all_songs = []
        all_cultures = []
        
        for concert in concerts:
            print(f"   콘서트: {concert.title}")
            
            # 셋리스트 수집
            setlists = collector.get_concert_setlists(concert.title, artist_name)
            all_setlists.extend(setlists)
            time.sleep(Config.REQUEST_DELAY)
            
            # 각 셋리스트의 곡 정보 수집
            for setlist in setlists:
                setlist_songs, songs = collector.get_setlist_songs(setlist.title, artist_name)
                all_setlist_songs.extend(setlist_songs)
                all_songs.extend(songs)
                time.sleep(Config.REQUEST_DELAY)
            
            # 문화적 맥락 수집
            culture = collector.get_concert_culture(concert.title, artist_name)
            if culture:
                all_cultures.append(culture)
            time.sleep(Config.REQUEST_DELAY)
        
        print(f"   {len(all_setlists)}개의 셋리스트 정보를 수집했습니다.")
        print(f"   {len(all_setlist_songs)}개의 셋리스트 곡 정보를 수집했습니다.")
        print(f"   {len(all_songs)}개의 곡 정보를 수집했습니다.")
        print(f"   {len(all_cultures)}개의 문화 정보를 수집했습니다.")
        
        # 3. CSV 파일로 저장
        print("3. CSV 파일 저장 중...")
        CSVDataManager.save_concerts(concerts)
        CSVDataManager.save_setlists(all_setlists)
        CSVDataManager.save_setlist_songs(all_setlist_songs)
        CSVDataManager.save_songs(all_songs)
        CSVDataManager.save_cultures(all_cultures)
        
        print(f"\n✅ 완료! {artist_name}의 데이터가 CSV 파일로 저장되었습니다.")
        print(f"📊 수집 결과:")
        print(f"   - 콘서트: {len(concerts)}개")
        print(f"   - 셋리스트: {len(all_setlists)}개")
        print(f"   - 셋리스트 곡: {len(all_setlist_songs)}개")
        print(f"   - 곡: {len(all_songs)}개")
        print(f"   - 문화 정보: {len(all_cultures)}개")
        print(f"\n📁 파일 위치: {Config.OUTPUT_DIR}/")
        
    except ValueError as e:
        logger.error(f"설정 오류: {e}")
        print(f"설정 오류: {e}")
    except KeyboardInterrupt:
        print("\n작업이 사용자에 의해 중단되었습니다.")
    except Exception as e:
        logger.error(f"예상치 못한 오류 발생: {e}")
        print(f"오류가 발생했습니다: {e}")

if __name__ == "__main__":
    main()
