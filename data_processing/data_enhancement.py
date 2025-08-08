import pandas as pd
import os
import logging
from typing import Dict, List, Optional, Any
from src.perplexity_api import PerplexityAPI
from config import Config
import time

logger = logging.getLogger(__name__)

class DataEnhancement:
    """빈 정보를 퍼플렉시티 API로 채우기 위한 클래스"""
    
    def __init__(self, perplexity_api: PerplexityAPI):
        self.api = perplexity_api
    
    def enhance_all_csv_files(self):
        """모든 CSV 파일의 빈 정보를 채웁니다."""
        csv_files = [
            'artists.csv',
            'concerts.csv', 
            'md.csv',
            'schedule.csv',
            'cultures.csv',
            'concert_setlists.csv',
            'setlists.csv',
            'setlist_songs.csv',
            'songs.csv'
        ]
        
        for csv_file in csv_files:
            filepath = os.path.join(Config.OUTPUT_DIR, csv_file)
            if os.path.exists(filepath):
                logger.info(f"{csv_file} 빈 정보 채우기 시작")
                self._enhance_csv_file(filepath, csv_file)
            else:
                logger.warning(f"{csv_file} 파일이 존재하지 않습니다.")
    
    def _enhance_csv_file(self, filepath: str, filename: str):
        """개별 CSV 파일의 빈 정보를 채웁니다."""
        try:
            df = pd.read_csv(filepath, encoding='utf-8-sig')
            
            if df.empty:
                logger.warning(f"{filename}이 비어있습니다.")
                return
            
            # 파일별 특화 처리
            if filename == 'artists.csv':
                df = self._enhance_artists(df)
            elif filename == 'concerts.csv':
                df = self._enhance_concerts(df)
            elif filename == 'md.csv':
                df = self._enhance_merchandise(df)
            elif filename == 'schedule.csv':
                df = self._enhance_schedules(df)
            elif filename == 'songs.csv':
                df = self._enhance_songs(df)
            
            # 수정된 데이터 저장
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            logger.info(f"{filename} 빈 정보 채우기 완료")
            
        except Exception as e:
            logger.error(f"{filename} 처리 실패: {e}")
    
    def _enhance_artists(self, df: pd.DataFrame) -> pd.DataFrame:
        """아티스트 정보의 빈 필드를 채웁니다."""
        for idx, row in df.iterrows():
            artist_name = row['artist']
            if not artist_name or pd.isna(artist_name):
                continue
            
            # 빈 필드들을 찾아서 채우기
            empty_fields = []
            if not row.get('birth_date') or pd.isna(row.get('birth_date')):
                empty_fields.append('birth_date')
            if not row.get('birth_place') or pd.isna(row.get('birth_place')):
                empty_fields.append('birth_place')
            if not row.get('category') or pd.isna(row.get('category')):
                empty_fields.append('category')
            if not row.get('detail') or pd.isna(row.get('detail')):
                empty_fields.append('detail')
            if not row.get('instagram_url') or pd.isna(row.get('instagram_url')):
                empty_fields.append('instagram_url')
            if not row.get('keywords') or pd.isna(row.get('keywords')):
                empty_fields.append('keywords')
            if not row.get('img_url') or pd.isna(row.get('img_url')):
                empty_fields.append('img_url')
            
            if empty_fields:
                logger.info(f"{artist_name} 아티스트의 빈 필드 채우기: {empty_fields}")
                enhanced_data = self._query_artist_info(artist_name, empty_fields)
                
                for field, value in enhanced_data.items():
                    if field in empty_fields and value:
                        # detail 필드의 경우 출처 표기 제거
                        if field == 'detail':
                            value = self._remove_sources_from_text(str(value))
                        # keywords 필드의 경우 아티스트 이름 제거
                        elif field == 'keywords':
                            value = self._filter_artist_name_from_keywords(str(value), artist_name)
                        
                        df.at[idx, field] = value
                
                time.sleep(Config.REQUEST_DELAY)
        
        return df
    
    def _enhance_concerts(self, df: pd.DataFrame) -> pd.DataFrame:
        """콘서트 정보의 빈 필드를 채웁니다."""
        for idx, row in df.iterrows():
            concert_title = row['title']
            artist = row['artist']
            
            # 티켓 정보가 비어있는 경우 채우기
            if (not row.get('ticket_site') or pd.isna(row.get('ticket_site'))) and \
               (not row.get('ticket_url') or pd.isna(row.get('ticket_url'))):
                logger.info(f"{concert_title} 티켓 정보 채우기")
                ticket_info = self._query_ticket_info(concert_title, artist)
                
                if ticket_info.get('site'):
                    df.at[idx, 'ticket_site'] = ticket_info['site']
                if ticket_info.get('url'):
                    df.at[idx, 'ticket_url'] = ticket_info['url']
                
                time.sleep(Config.REQUEST_DELAY)
        
        return df
    
    def _enhance_merchandise(self, df: pd.DataFrame) -> pd.DataFrame:
        """MD 상품 정보의 가격 형식을 통일합니다."""
        for idx, row in df.iterrows():
            price = row.get('price', '')
            if price and not pd.isna(price):
                # 가격을 nn,nnn원 형식으로 변환
                import re
                numbers = re.findall(r'\d+', str(price).replace(',', ''))
                if numbers:
                    num = int(numbers[0])
                    formatted_price = f"{num:,}원"
                    df.at[idx, 'price'] = formatted_price
        
        return df
    
    def _enhance_schedules(self, df: pd.DataFrame) -> pd.DataFrame:
        """스케줄 정보를 보완합니다."""
        for idx, row in df.iterrows():
            concert_title = row['concert_title']
            
            # 카테고리나 시간이 비어있는 경우 채우기
            if (not row.get('category') or pd.isna(row.get('category'))) or \
               (not row.get('schedule_at') or pd.isna(row.get('schedule_at'))):
                logger.info(f"{concert_title} 스케줄 정보 채우기")
                schedule_info = self._query_schedule_info(concert_title)
                
                if schedule_info and len(schedule_info) > 0:
                    # 첫 번째 스케줄 정보로 업데이트
                    first_schedule = schedule_info[0]
                    if not row.get('category') or pd.isna(row.get('category')):
                        df.at[idx, 'category'] = first_schedule.get('category', '')
                    if not row.get('schedule_at') or pd.isna(row.get('schedule_at')):
                        df.at[idx, 'schedule_at'] = first_schedule.get('schedule_at', '')
                
                time.sleep(Config.REQUEST_DELAY)
        
        return df
    
    def _enhance_songs(self, df: pd.DataFrame) -> pd.DataFrame:
        """곡 정보의 유튜브 ID를 채웁니다."""
        for idx, row in df.iterrows():
            song_title = row['title']
            artist = row['artist']
            
            if not row.get('youtube_id') or pd.isna(row.get('youtube_id')):
                logger.info(f"{artist} - {song_title} 유튜브 ID 찾기")
                youtube_id = self._query_youtube_id(song_title, artist)
                
                if youtube_id:
                    df.at[idx, 'youtube_id'] = youtube_id
                
                time.sleep(Config.REQUEST_DELAY)
        
        return df
    
    def _query_artist_info(self, artist_name: str, empty_fields: List[str]) -> Dict[str, str]:
        """아티스트 정보를 퍼플렉시티로 조회합니다."""
        prompt = f""""{artist_name}" 아티스트의 다음 정보를 검색해주세요:

필요한 정보: {', '.join(empty_fields)}

규칙:
- artist: "원어 (한국어)" 형식으로 작성 (예: "IU (아이유)", "BTS (방탄소년단)")
- birth_date: 데뷔연도 또는 첫 앨범 출간연도 (정수 형식)
- detail: 반드시 해요체(~해요, ~이에요, ~돼요)로 작성하고, 출처나 참조 표시([1], [2], URL 등)는 절대 포함하지 마세요
- keywords: 장르, 스타일, 특징만 포함하고 아티스트 이름은 절대 포함하지 마세요 (예: "록,팝,발라드")
- img_url: 가장 대표적이고 고화질인 공식 프로필 사진 URL
- 정보를 찾을 수 없으면 해당 필드는 빈 문자열 또는 0으로 설정

JSON 형식으로만 답변:
{{"artist": "원어 (한국어) 형식", "birth_date": "데뷔연도(정수) 또는 0", "birth_place": "출생지 또는 빈문자열", "category": "카테고리 또는 빈문자열", "detail": "해요체 상세설명 (출처 표시 없음)", "instagram_url": "URL 또는 빈문자열", "keywords": "장르나 특징만 (아티스트명 제외)", "img_url": "대표적인 고화질 이미지URL 또는 빈문자열"}}

JSON만 반환하세요."""
        
        try:
            response = self.api.query_with_search(prompt)
            return self._parse_json_response(response)
        except Exception as e:
            logger.error(f"아티스트 정보 조회 실패 ({artist_name}): {e}")
            return {}
    
    def _query_ticket_info(self, concert_title: str, artist: str) -> Dict[str, str]:
        """티켓 정보를 퍼플렉시티로 조회합니다."""
        prompt = f""""{artist}"의 "{concert_title}" 콘서트 티켓 예매 정보를 찾아주세요.

JSON 형식으로만 답변:
{{"ticket_site": "사이트명 또는 빈문자열", "ticket_url": "예매URL 또는 빈문자열"}}

JSON만 반환하세요."""
        
        try:
            response = self.api.query_with_search(prompt)
            return self._parse_json_response(response)
        except Exception as e:
            logger.error(f"티켓 정보 조회 실패 ({concert_title}): {e}")
            return {}
    
    def _query_schedule_info(self, concert_title: str) -> List[Dict[str, str]]:
        """스케줄 정보를 퍼플렉시티로 조회합니다."""
        prompt = f""""{concert_title}" 콘서트의 상세 일정을 찾아주세요.

찾을 일정:
- 티켓팅
- 추가 티켓팅
- 공연 시간
- 입장 시간
- 기타 콘서트 관련 일정

JSON 배열 형식으로만 답변:
[{{"category": "일정명", "schedule_at": "YYYY-MM-DD HH:MM:SS"}}]

JSON 배열만 반환하세요."""
        
        try:
            response = self.api.query_with_search(prompt)
            return self._parse_json_array_response(response)
        except Exception as e:
            logger.error(f"스케줄 정보 조회 실패 ({concert_title}): {e}")
            return []
    
    def _query_youtube_id(self, song_title: str, artist: str) -> str:
        """유튜브 ID를 퍼플렉시티로 조회합니다."""
        prompt = f""""{artist}"의 "{song_title}" 곡의 유튜브 공식 영상 ID를 찾아주세요.

유튜브 URL에서 ID 부분만 추출해주세요.
예: https://www.youtube.com/watch?v=fcQ37Ys4wpE → fcQ37Ys4wpE

JSON 형식으로만 답변:
{{"youtube_id": "유튜브ID 또는 빈문자열"}}

JSON만 반환하세요."""
        
        try:
            response = self.api.query_with_search(prompt)
            data = self._parse_json_response(response)
            return data.get('youtube_id', '')
        except Exception as e:
            logger.error(f"유튜브 ID 조회 실패 ({artist} - {song_title}): {e}")
            return ''
    
    def _parse_json_response(self, response: str) -> Dict[str, str]:
        """JSON 응답을 파싱합니다."""
        import json
        import re
        
        try:
            # JSON 객체 패턴 찾기
            json_match = re.search(r'\{[^}]*\}', response, re.DOTALL)
            if json_match:
                json_str = json_match.group()
                return json.loads(json_str)
        except Exception as e:
            logger.error(f"JSON 파싱 실패: {e}")
        
        return {}
    
    def _parse_json_array_response(self, response: str) -> List[Dict[str, str]]:
        """JSON 배열 응답을 파싱합니다."""
        import json
        import re
        
        try:
            # JSON 배열 패턴 찾기
            json_match = re.search(r'\[[^\]]*\]', response, re.DOTALL)
            if json_match:
                json_str = json_match.group()
                return json.loads(json_str)
        except Exception as e:
            logger.error(f"JSON 배열 파싱 실패: {e}")
        
        return []
    
    def _remove_sources_from_text(self, text: str) -> str:
        """텍스트에서 출처 표시 제거"""
        import re
        if not text:
            return text
        
        # 다양한 출처 패턴 제거
        patterns = [
            r'\[출처:.*?\]',  # [출처: ...]
            r'\(출처:.*?\)',  # (출처: ...)
            r'\[.*?\d{4}-\d{2}-\d{2}.*?\]',  # [사이트명 2024-01-01]
            r'\[.*?https?://.*?\]',  # [URL 포함]
            r'https?://[^\s\]]+',  # 직접 URL
            r'\[\d+\]',  # [1], [2] 등 참조 번호
            r'\s*\([^)]*2025[^)]*\)',  # (2025 포함 괄호)
            r'\s*\([^)]*\d{4}-\d{2}-\d{2}[^)]*\)',  # 날짜 포함 괄호
        ]
        
        cleaned_text = text
        for pattern in patterns:
            cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.IGNORECASE)
        
        # 연속된 공백 정리
        cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
        
        return cleaned_text
    
    def _filter_artist_name_from_keywords(self, keywords: str, artist_name: str) -> str:
        """키워드에서 아티스트 이름 제거"""
        if not keywords or not artist_name:
            return keywords
        
        import re
        
        # 키워드를 쉼표로 분리
        keyword_list = [k.strip() for k in keywords.split(',')]
        
        # 아티스트 이름에서 특수문자 제거하여 비교용 이름 생성
        clean_artist_name = re.sub(r'[^\w\s]', '', artist_name.lower())
        artist_words = clean_artist_name.split()
        
        # 아티스트 이름이 포함된 키워드 제거
        filtered_keywords = []
        for keyword in keyword_list:
            if not keyword:
                continue
            
            clean_keyword = re.sub(r'[^\w\s]', '', keyword.lower())
            
            # 키워드가 아티스트 이름의 일부와 일치하는지 확인
            is_artist_name = False
            for artist_word in artist_words:
                if len(artist_word) > 2 and artist_word in clean_keyword:
                    is_artist_name = True
                    break
            
            # 아티스트 이름이 아닌 경우에만 키워드 유지
            if not is_artist_name:
                filtered_keywords.append(keyword)
        
        return ','.join(filtered_keywords)