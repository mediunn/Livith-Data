import pandas as pd
import os
import logging
from typing import List, Dict, Any
from src.data_models import *
from config import Config

logger = logging.getLogger(__name__)

class EnhancedCSVManager:
    @staticmethod
    def ensure_output_dir():
        if not os.path.exists(Config.OUTPUT_DIR):
            os.makedirs(Config.OUTPUT_DIR)
    
    @staticmethod
    def save_all_data(collected_data: List[Dict[str, Any]]):
        """모든 수집된 데이터를 CSV로 저장"""
        EnhancedCSVManager.ensure_output_dir()
        
        # 각 데이터 타입별로 수집
        all_concerts = []
        all_setlists = []
        all_concert_setlists = []
        all_setlist_songs = []
        all_songs = []
        all_cultures = []
        all_schedules = []
        all_merchandise = []
        all_concert_info = []
        all_artists = []
        
        for data in collected_data:
            if data['concert']:
                all_concerts.append(data['concert'])
            all_setlists.extend(data['setlists'])
            all_concert_setlists.extend(data['concert_setlists'])
            all_setlist_songs.extend(data['setlist_songs'])
            all_songs.extend(data['songs'])
            all_cultures.extend(data['cultures'])
            all_schedules.extend(data['schedules'])
            all_merchandise.extend(data['merchandise'])
            all_concert_info.extend(data['concert_info'])
            if data['artist']:
                all_artists.append(data['artist'])
        
        # CSV 파일로 저장
        EnhancedCSVManager._save_to_csv(all_concerts, "concerts.csv", "콘서트")
        EnhancedCSVManager._save_to_csv(all_setlists, "setlists.csv", "셋리스트")
        EnhancedCSVManager._save_to_csv(all_concert_setlists, "concert_setlists.csv", "콘서트-셋리스트")
        EnhancedCSVManager._save_to_csv(all_setlist_songs, "setlist_songs.csv", "셋리스트 곡")
        EnhancedCSVManager._save_to_csv(all_songs, "songs.csv", "곡")
        EnhancedCSVManager._save_to_csv(all_cultures, "cultures.csv", "문화")
        EnhancedCSVManager._save_to_csv(all_schedules, "schedule.csv", "스케줄")
        EnhancedCSVManager._save_to_csv(all_merchandise, "md.csv", "MD")
        EnhancedCSVManager._save_to_csv(all_concert_info, "concert_info.csv", "콘서트 정보")
        EnhancedCSVManager._save_to_csv(all_artists, "artists.csv", "아티스트")
    
    @staticmethod
    def _save_to_csv(data: List, filename: str, description: str):
        """CSV 저장 공통 메서드 - 데이터 모델 필드만 저장"""
        if not data:
            logger.warning(f"{description} 데이터가 없습니다.")
            return
        
        filepath = os.path.join(Config.OUTPUT_DIR, filename)
        
        # 데이터 모델의 필드만 추출하여 DataFrame 생성
        clean_data = []
        for item in data:
            if hasattr(item, '__dataclass_fields__'):
                # dataclass 객체인 경우 정의된 필드만 추출
                item_dict = {}
                for field_name in item.__dataclass_fields__.keys():
                    item_dict[field_name] = getattr(item, field_name, '')
                clean_data.append(item_dict)
            else:
                # 일반 객체인 경우 vars() 사용
                clean_data.append(vars(item))
        
        df = pd.DataFrame(clean_data)
        
        df.to_csv(
            filepath,
            index=False,
            encoding='utf-8-sig',
            escapechar='\\',
            quoting=1
        )
        
        logger.info(f"{description} 데이터를 {filepath}에 저장했습니다. ({len(data)}개)")
