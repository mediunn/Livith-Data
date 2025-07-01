import pandas as pd
import os
import logging
from typing import List
from src.data_models import Concert, Setlist, SetlistSong, Song, Culture
from config import Config

logger = logging.getLogger(__name__)

class CSVDataManager:
    @staticmethod
    def ensure_output_dir():
        if not os.path.exists(Config.OUTPUT_DIR):
            os.makedirs(Config.OUTPUT_DIR)
    
    @staticmethod
    def _save_to_csv(data: List, filename: str, description: str):
        """공통 CSV 저장 메서드"""
        CSVDataManager.ensure_output_dir()
        filepath = os.path.join(Config.OUTPUT_DIR, filename)
        
        df = pd.DataFrame([vars(item) for item in data])
        
        df.to_csv(
            filepath, 
            index=False, 
            encoding='utf-8-sig',
            escapechar='\\',
            quoting=1
        )
        
        logger.info(f"{description} 데이터를 {filepath}에 저장했습니다.")
        
        try:
            test_df = pd.read_csv(filepath, encoding='utf-8-sig')
            logger.info(f"저장 검증 완료: {len(test_df)}개 행이 올바르게 저장되었습니다.")
        except Exception as e:
            logger.warning(f"저장된 파일 검증 중 오류: {e}")
    
    @staticmethod
    def save_concerts(concerts: List[Concert], filename: str = "concerts.csv"):
        CSVDataManager._save_to_csv(concerts, filename, "콘서트")
    
    @staticmethod
    def save_setlists(setlists: List[Setlist], filename: str = "setlists.csv"):
        CSVDataManager._save_to_csv(setlists, filename, "셋리스트")
    
    @staticmethod
    def save_setlist_songs(setlist_songs: List[SetlistSong], filename: str = "setlist_songs.csv"):
        CSVDataManager._save_to_csv(setlist_songs, filename, "셋리스트 곡")
    
    @staticmethod
    def save_songs(songs: List[Song], filename: str = "songs.csv"):
        CSVDataManager._save_to_csv(songs, filename, "곡")
    
    @staticmethod
    def save_cultures(cultures: List[Culture], filename: str = "cultures.csv"):
        CSVDataManager._save_to_csv(cultures, filename, "문화")
