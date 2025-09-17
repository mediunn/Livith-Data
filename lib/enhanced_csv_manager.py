"""
향상된 CSV 관리자 - 데이터 모델 기반 CSV 파일 생성 및 관리
"""
import pandas as pd
import os
import logging
from typing import List, Dict, Any
from lib.data_models import *
from lib.enhanced_data_collector import EnhancedDataCollector
from lib.config import Config
from lib.safe_writer import SafeWriter

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
        all_concert_info = []
        all_artists = []
        all_concert_genres = []
        
        for data in collected_data:
            if data['concert']:
                all_concerts.append(data['concert'])
            all_setlists.extend(data['setlists'])
            all_concert_setlists.extend(data['concert_setlists'])
            all_setlist_songs.extend(data['setlist_songs'])
            all_songs.extend(data['songs'])
            all_cultures.extend(data['cultures'])
            all_schedules.extend(data['schedules'])
            all_concert_info.extend(data['concert_info'])
            all_concert_genres.extend(data['concert_genres'])
            logger.info(f"콘서트 장르 데이터 추가됨: {len(data['concert_genres'])}개")
            if data['artist']:
                all_artists.append(data['artist'])
                logger.info(f"아티스트 데이터 추가됨: {data['artist'].artist}")
            else:
                logger.warning(f"아티스트 데이터가 None입니다.")
        
        # 콘서트 데이터 정렬
        if all_concerts:
            # 기존 데이터 로드
            existing_concerts = EnhancedCSVManager._load_existing_data("concerts.csv")
            
            # 스마트 병합: KOPIS 데이터는 보존하고 새 필드는 업데이트
            merged_concerts = {}
            
            # 기존 데이터를 먼저 저장 (KOPIS 데이터 포함)
            for concert in existing_concerts:
                merged_concerts[concert.title] = concert
            
            # 새 데이터로 업데이트 (KOPIS 데이터가 있으면 보존)
            for new_concert in all_concerts:
                if new_concert.title in merged_concerts:
                    # 기존 콘서트가 있으면 KOPIS 데이터는 보존하고 새 필드만 업데이트
                    existing = merged_concerts[new_concert.title]
                    merged_concerts[new_concert.title] = Concert(
                        artist=existing.artist,  # 기존 값 유지
                        code=existing.code or new_concert.code,  # 기존 값이 있으면 우선, 없으면 새 값
                        title=new_concert.title,
                        start_date=existing.start_date or new_concert.start_date,
                        end_date=existing.end_date or new_concert.end_date,
                        status=new_concert.status,  # 상태는 새 값으로 업데이트
                        poster=existing.poster or new_concert.poster,  # KOPIS 데이터 보존
                        ticket_site=existing.ticket_site or new_concert.ticket_site,  # KOPIS 데이터 보존
                        ticket_url=existing.ticket_url or new_concert.ticket_url,  # KOPIS 데이터 보존
                        venue=existing.venue or new_concert.venue,  # KOPIS 데이터 보존
                        label=new_concert.label,  # 새 필드는 업데이트
                        introduction=new_concert.introduction  # 새 필드는 업데이트
                    )
                else:
                    # 새 콘서트면 그대로 추가
                    merged_concerts[new_concert.title] = new_concert
            
            unique_concerts = list(merged_concerts.values())
            
            # 콘서트 정렬
            sorted_concerts = EnhancedDataCollector.sort_concerts(unique_concerts)
            
            # 전체 데이터로 콘서트 저장 (overwrite)
            EnhancedCSVManager._save_to_csv(sorted_concerts, "concerts.csv", "콘서트", mode="overwrite")
        
        # 테스트 모드에서는 모든 데이터를 overwrite, 프로덕션에서는 append
        save_mode = "overwrite" if Config.OUTPUT_DIR == Config.TEST_OUTPUT_DIR else "append"
        
        if save_mode == "overwrite":
            # 테스트 모드: 전체 데이터 overwrite
            EnhancedCSVManager._save_to_csv(all_setlists, "setlists.csv", "셋리스트", mode="overwrite")
            EnhancedCSVManager._save_to_csv(all_concert_setlists, "concert_setlists.csv", "콘서트-셋리스트", mode="overwrite")
            EnhancedCSVManager._save_to_csv(all_setlist_songs, "setlist_songs.csv", "셋리스트 곡", mode="overwrite")
            EnhancedCSVManager._save_to_csv(all_songs, "songs.csv", "곡", mode="overwrite")
            EnhancedCSVManager._save_to_csv(all_cultures, "cultures.csv", "문화", mode="overwrite")
            EnhancedCSVManager._save_to_csv(all_schedules, "schedule.csv", "스케줄", mode="overwrite")
            EnhancedCSVManager._save_to_csv(all_concert_info, "concert_info.csv", "콘서트 정보", mode="overwrite")
            EnhancedCSVManager._save_to_csv(all_concert_genres, "concert_genres.csv", "콘서트 장르", mode="overwrite")
            EnhancedCSVManager._save_to_csv(all_artists, "artists.csv", "아티스트", mode="overwrite")
        else:
            # 프로덕션 모드: append로 저장 (기존 방식)
            EnhancedCSVManager._append_to_csv(all_setlists, "setlists.csv", "셋리스트")
            EnhancedCSVManager._append_to_csv(all_concert_setlists, "concert_setlists.csv", "콘서트-셋리스트")
            EnhancedCSVManager._append_to_csv(all_setlist_songs, "setlist_songs.csv", "셋리스트 곡")
            EnhancedCSVManager._append_to_csv(all_songs, "songs.csv", "곡")
            EnhancedCSVManager._append_to_csv(all_cultures, "cultures.csv", "문화")
            EnhancedCSVManager._append_to_csv(all_schedules, "schedule.csv", "스케줄")
            EnhancedCSVManager._append_to_csv(all_concert_info, "concert_info.csv", "콘서트 정보")
            EnhancedCSVManager._append_to_csv(all_concert_genres, "concert_genres.csv", "콘서트 장르")
            EnhancedCSVManager._append_to_csv(all_artists, "artists.csv", "아티스트")
    
    @staticmethod
    def _load_existing_data(filename: str) -> List[Concert]:
        """기존 CSV 데이터를 로드하여 Concert 객체 리스트 반환"""
        filepath = os.path.join(Config.OUTPUT_DIR, filename)
        if not os.path.exists(filepath):
            return []
        
        try:
            df = pd.read_csv(filepath, encoding='utf-8-sig')
            # NaN 값을 빈 문자열로 치환
            df = df.fillna('')
            
            concerts = []
            for _, row in df.iterrows():
                concert = Concert(
                    artist=str(row.get('artist', '')),
                    code=str(row.get('code', '')),
                    title=str(row.get('title', '')),
                    start_date=str(row.get('start_date', '')),
                    end_date=str(row.get('end_date', '')),
                    status=str(row.get('status', '')),
                    poster=str(row.get('poster', '')),
                    ticket_site=str(row.get('ticket_site', '')),
                    ticket_url=str(row.get('ticket_url', '')),
                    venue=str(row.get('venue', '')),
                    label=str(row.get('label', '')),
                    introduction=str(row.get('introduction', ''))
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
            logger.warning(f"{description} 데이터가 없습니다. 적절한 헤더로 빈 파일 생성.")
            filepath = os.path.join(Config.OUTPUT_DIR, filename)
            
            # 파일별 헤더 정의
            headers_map = {
                'artists.csv': ['artist', 'debut_date', 'category', 'detail', 'instagram_url', 'keywords', 'img_url'],
                'concerts.csv': ['artist', 'code', 'title', 'start_date', 'end_date', 'status', 'poster', 'ticket_site', 'ticket_url', 'venue', 'label', 'introduction'],
                'songs.csv': ['title', 'artist', 'lyrics', 'pronunciation', 'translation', 'youtube_id'],
                'setlists.csv': ['title', 'start_date', 'end_date', 'img_url', 'artist', 'venue'],
                'setlist_songs.csv': ['setlist_title', 'song_title', 'setlist_date', 'order_index', 'fanchant', 'fanchant_point'],
                'concert_setlists.csv': ['concert_title', 'setlist_title', 'type', 'status'],
                'cultures.csv': ['concert_title', 'title', 'content', 'img_url'],
                'schedule.csv': ['concert_title', 'category', 'scheduled_at'],
                'concert_info.csv': ['concert_title', 'category', 'content', 'img_url'],
                'concert_genres.csv': ['concert_id', 'concert_title', 'genre_id', 'name'],
            }
            
            headers = headers_map.get(filename, [])
            df = pd.DataFrame(columns=headers)
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
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
                    
                    # debut_date는 이미 문자열이므로 특별한 처리 불필요
                    
                    item_dict[field_name] = value
                clean_data.append(item_dict)
            else:
                # 일반 객체인 경우 vars() 사용
                item_data = vars(item).copy()
                
                # debut_date는 이미 문자열이므로 특별한 처리 불필요
                
                clean_data.append(item_data)
        
        df = pd.DataFrame(clean_data)
        
        # 메인 출력 디렉토리인 경우 백업 생성
        if Config.OUTPUT_DIR == Config.MAIN_OUTPUT_DIR and os.path.exists(filepath):
            backup_path = SafeWriter._create_backup_if_needed(filename)
            if backup_path:
                logger.info(f"📋 백업 생성: {os.path.basename(backup_path)}")
        
        df.to_csv(
            filepath,
            index=False,
            encoding='utf-8-sig',
            escapechar='\\',
            quoting=0  # QUOTE_MINIMAL로 변경 (필요한 경우에만 따옴표 사용)
        )
        
        logger.info(f"💾 {description} 데이터를 {filepath}에 저장했습니다. ({len(data)}개)")

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
                    
                    # debut_date는 이미 문자열이므로 특별한 처리 불필요
                    
                    item_dict[field_name] = value
                clean_data.append(item_dict)
            else:
                # 일반 객체인 경우 vars() 사용
                item_data = vars(item).copy()
                
                # debut_date는 이미 문자열이므로 특별한 처리 불필요
                
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
        
        # 메인 출력 디렉토리인 경우 백업 생성
        if Config.OUTPUT_DIR == Config.MAIN_OUTPUT_DIR and os.path.exists(filepath):
            backup_path = SafeWriter._create_backup_if_needed(filename)
            if backup_path:
                logger.info(f"📋 백업 생성: {os.path.basename(backup_path)}")
        
        combined_df.to_csv(
            filepath,
            index=False,
            encoding='utf-8-sig',
            escapechar='\\',
            quoting=0  # QUOTE_MINIMAL로 변경 (필요한 경우에만 따옴표 사용)
        )
        
        logger.info(f"💾 {description} 데이터를 {filepath}에 추가했습니다. ({len(data)}개 추가, 총 {len(combined_df)}개)")
