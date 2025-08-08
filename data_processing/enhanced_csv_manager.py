import pandas as pd
import os
import logging
from typing import List, Dict, Any
from src.data_models import *
from src.enhanced_data_collector import EnhancedDataCollector
from config import Config

logger = logging.getLogger(__name__)

class EnhancedCSVManager:
    @staticmethod
    def ensure_output_dir():
        if not os.path.exists(Config.OUTPUT_DIR):
            os.makedirs(Config.OUTPUT_DIR)
    
    @staticmethod
    def save_all_data(collected_data: List[Dict[str, Any]]):
        """모든 수집된 데이터를 CSV로 저장 (append 모드)"""
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
        
        # 콘서트 데이터에 sorted_index 계산
        if all_concerts:
            # 기존 데이터 로드
            existing_concerts = EnhancedCSVManager._load_existing_data("concerts.csv")
            
            # 새 데이터와 기존 데이터 병합
            combined_concerts = existing_concerts + all_concerts
            
            # 중복 제거 (title 기준)
            unique_concerts = []
            seen_titles = set()
            for concert in combined_concerts:
                if concert.title not in seen_titles:
                    unique_concerts.append(concert)
                    seen_titles.add(concert.title)
            
            # sorted_index 계산
            sorted_concerts = EnhancedDataCollector.calculate_sorted_indices(unique_concerts)
            
            # 전체 데이터로 콘서트 저장 (overwrite)
            EnhancedCSVManager._save_to_csv(sorted_concerts, "concerts.csv", "콘서트", mode="overwrite")
        
        # 나머지 데이터는 append 모드로 저장
        EnhancedCSVManager._append_to_csv(all_setlists, "setlists.csv", "셋리스트")
        EnhancedCSVManager._append_to_csv(all_concert_setlists, "concert_setlists.csv", "콘서트-셋리스트")
        EnhancedCSVManager._append_to_csv(all_setlist_songs, "setlist_songs.csv", "셋리스트 곡")
        EnhancedCSVManager._append_to_csv(all_songs, "songs.csv", "곡")
        EnhancedCSVManager._append_to_csv(all_cultures, "cultures.csv", "문화")
        EnhancedCSVManager._append_to_csv(all_schedules, "schedule.csv", "스케줄")
        EnhancedCSVManager._append_to_csv(all_merchandise, "md.csv", "MD")
        EnhancedCSVManager._append_to_csv(all_concert_info, "concert_info.csv", "콘서트 정보")
        EnhancedCSVManager._append_to_csv(all_artists, "artists.csv", "아티스트")
    
    @staticmethod
    def _load_existing_data(filename: str) -> List[Concert]:
        """기존 CSV 데이터를 로드하여 Concert 객체 리스트 반환"""
        filepath = os.path.join(Config.OUTPUT_DIR, filename)
        if not os.path.exists(filepath):
            return []
        
        try:
            df = pd.read_csv(filepath, encoding='utf-8-sig')
            concerts = []
            for _, row in df.iterrows():
                concert = Concert(
                    artist=row.get('artist', ''),  # 기존 artist_display 내용
                    code=row.get('code', ''),
                    title=row.get('title', ''),
                    start_date=row.get('start_date', ''),
                    end_date=row.get('end_date', ''),
                    status=row.get('status', ''),
                    poster=row.get('poster', ''),
                    sorted_index=int(row.get('sorted_index', 0)),
                    ticket_site=row.get('ticket_site', ''),
                    ticket_url=row.get('ticket_url', ''),
                    venue=row.get('venue', '')
                )
                concerts.append(concert)
            return concerts
        except Exception as e:
            logger.error(f"기존 데이터 로드 실패: {e}")
            return []

    @staticmethod
    def _save_to_csv(data: List, filename: str, description: str, mode: str = "overwrite"):
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
                    value = getattr(item, field_name, '')
                    
                    # Artist 객체의 birth_date 처리
                    if (hasattr(item, 'birth_date') and field_name == 'birth_date'):
                        if value == 0:
                            value = ''
                        elif isinstance(value, float):
                            value = int(value)  # float을 정수로 변환
                    
                    item_dict[field_name] = value
                clean_data.append(item_dict)
            else:
                # 일반 객체인 경우 vars() 사용
                item_data = vars(item).copy()
                
                # Artist 객체의 birth_date 처리
                if 'birth_date' in item_data:
                    if item_data['birth_date'] == 0:
                        item_data['birth_date'] = ''
                    elif isinstance(item_data['birth_date'], float):
                        item_data['birth_date'] = int(item_data['birth_date'])  # float을 정수로 변환
                
                clean_data.append(item_data)
        
        df = pd.DataFrame(clean_data)
        
        df.to_csv(
            filepath,
            index=False,
            encoding='utf-8-sig',
            escapechar='\\',
            quoting=0  # QUOTE_MINIMAL로 변경 (필요한 경우에만 따옴표 사용)
        )
        
        logger.info(f"{description} 데이터를 {filepath}에 저장했습니다. ({len(data)}개)")

    @staticmethod
    def _append_to_csv(data: List, filename: str, description: str):
        """CSV에 데이터 추가 (중복 제거)"""
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
                    value = getattr(item, field_name, '')
                    
                    # Artist 객체의 birth_date 처리
                    if (hasattr(item, 'birth_date') and field_name == 'birth_date'):
                        if value == 0:
                            value = ''
                        elif isinstance(value, float):
                            value = int(value)  # float을 정수로 변환
                    
                    item_dict[field_name] = value
                clean_data.append(item_dict)
            else:
                # 일반 객체인 경우 vars() 사용
                item_data = vars(item).copy()
                
                # Artist 객체의 birth_date 처리
                if 'birth_date' in item_data:
                    if item_data['birth_date'] == 0:
                        item_data['birth_date'] = ''
                    elif isinstance(item_data['birth_date'], float):
                        item_data['birth_date'] = int(item_data['birth_date'])  # float을 정수로 변환
                
                clean_data.append(item_data)
        
        new_df = pd.DataFrame(clean_data)
        
        # 기존 파일이 있으면 로드해서 병합
        if os.path.exists(filepath):
            try:
                existing_df = pd.read_csv(filepath, encoding='utf-8-sig')
                # 중복 제거를 위한 키 생성 (첫 번째 컬럼 기준)
                if not existing_df.empty and not new_df.empty:
                    key_column = new_df.columns[0]
                    if key_column in existing_df.columns:
                        # 데이터 업서트 (중복시 새 데이터로 업데이트)
                        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                        combined_df = combined_df.drop_duplicates(subset=[key_column], keep='last')
                    else:
                        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                else:
                    combined_df = new_df
            except Exception as e:
                logger.error(f"기존 파일 로드 실패: {e}")
                combined_df = new_df
        else:
            combined_df = new_df
        
        combined_df.to_csv(
            filepath,
            index=False,
            encoding='utf-8-sig',
            escapechar='\\',
            quoting=0  # QUOTE_MINIMAL로 변경 (필요한 경우에만 따옴표 사용)
        )
        
        logger.info(f"{description} 데이터를 {filepath}에 추가했습니다. ({len(data)}개 추가, 총 {len(combined_df)}개)")
