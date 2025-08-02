import json
import time
import logging
import re
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
from src.perplexity_api import PerplexityAPI
from src.data_models import *
from config import Config

logger = logging.getLogger(__name__)

class EnhancedDataCollector:
    def __init__(self, perplexity_api: PerplexityAPI):
        self.api = perplexity_api
    
    def collect_concert_data(self, kopis_concert: Dict[str, Any]) -> Dict[str, Any]:
        """KOPIS 콘서트 정보를 바탕으로 상세 데이터 수집"""
        concert_title = kopis_concert['title']
        artist_name = kopis_concert['artist']
        
        logger.info(f"데이터 수집 시작: {concert_title} - {artist_name}")
        
        # 기본 콘서트 정보 생성 - KOPIS 데이터를 데이터 모델에 맞게 매핑
        concert = Concert(
            title=concert_title,
            start_date=self._format_date(kopis_concert['start_date']),
            end_date=self._format_date(kopis_concert['end_date']),
            artist=artist_name,
            poster=kopis_concert.get('poster', ''),
            status=self._map_status_to_string(kopis_concert.get('status', '')),
            venue=kopis_concert.get('venue', ''),
            ticket_url=""  # KOPIS에서 제공하지 않음
        )
        
        # 상세 데이터 수집
        setlists = self._collect_setlists(concert_title, artist_name)
        concert_setlists = self._collect_concert_setlists(concert_title, setlists)
        setlist_songs, songs = self._collect_songs_data(setlists, artist_name)
        cultures = self._collect_cultures(concert_title, artist_name)
        schedules = self._collect_schedules(concert_title, artist_name, concert.start_date, concert.end_date)
        merchandise = self._collect_merchandise(concert_title, artist_name)
        concert_info = self._collect_concert_info(concert_title, artist_name)
        artist_info = self._collect_artist_info(artist_name)
        
        return {
            'concert': concert,
            'setlists': setlists,
            'concert_setlists': concert_setlists,
            'setlist_songs': setlist_songs,
            'songs': songs,
            'cultures': cultures,
            'schedules': schedules,
            'merchandise': merchandise,
            'concert_info': concert_info,
            'artist': artist_info
        }
    
    def _collect_setlists(self, concert_title: str, artist_name: str) -> List[Setlist]:
        """셋리스트 정보 수집"""
        prompt = f"""Search for verified setlist information for {artist_name}'s "{concert_title}" concert.

Find actual setlist details and provide ONLY valid JSON:
[{{"title": "Setlist Name", "start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD", "img_url": "URL", "artist": "{artist_name}", "venue": "Venue Location"}}]

Return only JSON array with no additional text."""
        
        response = self.api.query_with_search(prompt)
        return self._parse_setlists(response, concert_title, artist_name)
    
    def _collect_concert_setlists(self, concert_title: str, setlists: List[Setlist]) -> List[ConcertSetlist]:
        """콘서트-셋리스트 연결 정보 수집"""
        prompt = f"""Search for performance schedule information for "{concert_title}".

Find specific performance dates and status. Provide ONLY valid JSON:
[{{"concert_title": "{concert_title}", "setlist_title": "Setlist Name", "setlist_date": "YYYY-MM-DD", "type": "ONGOING/EXPECTED/PAST", "status": "1회차/2회차/종료"}}]

Return only JSON array."""
        
        response = self.api.query_with_search(prompt)
        return self._parse_concert_setlists(response, concert_title, setlists)
    
    def _collect_songs_data(self, setlists: List[Setlist], artist_name: str) -> Tuple[List[SetlistSong], List[Song]]:
        """곡 정보 수집"""
        all_setlist_songs = []
        all_songs = []
        
        for setlist in setlists:
            prompt = f"""Search for song list in {artist_name}'s "{setlist.title}" setlist.

Find actual songs performed. Provide ONLY valid JSON:
{{"setlist_songs": [{{"setlist_title": "{setlist.title}", "song_title": "Song Name", "setlist_date": "{setlist.start_date}", "order_index": 1, "fanchant": "", "venue": "{setlist.venue}"}}], "songs": [{{"title": "Song Name", "artist": "{artist_name}", "lyrics": "", "pronunciation": "", "translation": "", "youtube_id": "YouTube_ID"}}]}}

Return only JSON object."""
            
            response = self.api.query_with_search(prompt)
            setlist_songs, songs = self._parse_songs_data(response, setlist, artist_name)
            all_setlist_songs.extend(setlist_songs)
            all_songs.extend(songs)
            time.sleep(Config.REQUEST_DELAY)
        
        return all_setlist_songs, all_songs
    
    def _collect_cultures(self, concert_title: str, artist_name: str) -> List[Culture]:
        """문화 정보 수집"""
        prompt = f"""Search for fan culture and unique traditions related to {artist_name}'s "{concert_title}" concert.

Find specific fan behaviors, chants, or cultural elements. Provide ONLY valid JSON in Korean:
[{{"concert_title": "{concert_title}", "title": "문화 요소 제목", "content": "문화 요소에 대한 한국어 설명"}}]

Return only JSON array with Korean descriptions."""
        
        response = self.api.query_with_search(prompt)
        return self._parse_cultures(response, concert_title)
    
    def _collect_schedules(self, concert_title: str, artist_name: str, start_date: str, end_date: str) -> List[Schedule]:
        """스케줄 정보 수집"""
        prompt = f"""Search for detailed schedule information for {artist_name}'s "{concert_title}" concert from {start_date} to {end_date}.

Find specific performance times and events. Provide ONLY valid JSON:
[{{"concert_title": "{concert_title}", "category": "MM.DD(요일) 스케줄명", "scheduled_at": "YYYY-MM-DD HH:MM:SS"}}]

Return only JSON array."""
        
        response = self.api.query_with_search(prompt)
        return self._parse_schedules(response, concert_title)
    
    def _collect_merchandise(self, concert_title: str, artist_name: str) -> List[Merchandise]:
        """MD 상품 정보 수집"""
        prompt = f"""Search for official merchandise for {artist_name}'s "{concert_title}" concert.

Find concert-specific merchandise with prices. Provide ONLY valid JSON:
[{{"concert_title": "{concert_title}", "name": "상품명", "price": "가격", "img_url": "이미지URL"}}]

Return only JSON array."""
        
        response = self.api.query_with_search(prompt)
        return self._parse_merchandise(response, concert_title)
    
    def _collect_concert_info(self, concert_title: str, artist_name: str) -> List[ConcertInfo]:
        """콘서트 정보 수집"""
        prompt = f"""Search for important information about {artist_name}'s "{concert_title}" concert venue, seating, rules, etc.

Find practical concert information. Provide ONLY valid JSON in Korean:
[{{"concert_title": "{concert_title}", "category": "정보 카테고리", "content": "한국어 설명", "img_url": "관련 이미지URL"}}]

Return only JSON array with Korean content."""
        
        response = self.api.query_with_search(prompt)
        return self._parse_concert_info(response, concert_title)
    
    def _collect_artist_info(self, artist_name: str) -> Optional[Artist]:
        """아티스트 정보 수집"""
        prompt = f"""Search for biographical information about artist {artist_name}.

Find birth date, origin, category, and details. Provide ONLY valid JSON in Korean:
{{"artist": "{artist_name}", "birth_date": "YYYY-MM-DD", "birth_place": "출생지", "category": "아티스트 카테고리", "detail": "한국어 상세 설명", "instagram_url": "인스타그램URL", "keywords": "키워드1,키워드2,키워드3", "img_url": "프로필이미지URL"}}

Return only JSON object with Korean descriptions."""
        
        response = self.api.query_with_search(prompt)
        return self._parse_artist_info(response, artist_name)
    
    def _format_date(self, date_str: str) -> str:
        """날짜 형식 변환 (YYYYMMDD -> YYYY-MM-DD)"""
        if not date_str or len(date_str) != 8:
            return "2025-01-01"
        
        try:
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        except:
            return "2025-01-01"
    
    def _clean_json_response(self, response: str) -> str:
        """JSON 응답 정리"""
        if not response:
            return ""
        
        cleaned = response.strip()
        cleaned = re.sub(r"'([^']*)':", r'"\1":', cleaned)
        cleaned = re.sub(r":\s*'([^']*)'", r': "\1"', cleaned)
        cleaned = re.sub(r',\s*}', '}', cleaned)
        cleaned = re.sub(r',\s*]', ']', cleaned)
        cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)
        
        return cleaned
    
    def _extract_json_from_response(self, response: str, start_char: str, end_char: str) -> str:
        """응답에서 JSON 추출"""
        try:
            cleaned_response = self._clean_json_response(response)
            start_idx = cleaned_response.find(start_char)
            if start_idx == -1:
                return ""
            
            bracket_count = 0
            end_idx = start_idx
            
            for i, char in enumerate(cleaned_response[start_idx:], start_idx):
                if char == start_char:
                    bracket_count += 1
                elif char == end_char:
                    bracket_count -= 1
                    if bracket_count == 0:
                        end_idx = i + 1
                        break
            
            json_str = cleaned_response[start_idx:end_idx]
            
            try:
                json.loads(json_str)
                return json_str
            except json.JSONDecodeError:
                return ""
            
        except Exception as e:
            logger.error(f"JSON 추출 실패: {e}")
            return ""
    
    # 파싱 메서드들 (간단화)
    def _parse_setlists(self, response: str, concert_title: str, artist_name: str) -> List[Setlist]:
        try:
            json_str = self._extract_json_from_response(response, '[', ']')
            if json_str:
                data = json.loads(json_str)
                return [Setlist(**item) for item in data if isinstance(item, dict)]
        except:
            pass
        
        # 기본값 반환
        return [Setlist(
            title=f"{concert_title} Main Setlist",
            start_date="2025-01-01",
            end_date="2025-01-01",
            img_url="",
            artist=artist_name,
            venue=""
        )]
    
    def _parse_concert_setlists(self, response: str, concert_title: str, setlists: List[Setlist]) -> List[ConcertSetlist]:
        try:
            json_str = self._extract_json_from_response(response, '[', ']')
            if json_str:
                data = json.loads(json_str)
                return [ConcertSetlist(**item) for item in data if isinstance(item, dict)]
        except:
            pass
        
        # 기본값 반환
        return [ConcertSetlist(
            concert_title=concert_title,
            setlist_title=setlist.title,
            setlist_date=setlist.start_date,
            type="PAST",
            status="종료"
        ) for setlist in setlists]
    
    def _parse_songs_data(self, response: str, setlist: Setlist, artist_name: str) -> Tuple[List[SetlistSong], List[Song]]:
        try:
            json_str = self._extract_json_from_response(response, '{', '}')
            if json_str:
                data = json.loads(json_str)
                setlist_songs = [SetlistSong(**item) for item in data.get('setlist_songs', []) if isinstance(item, dict)]
                songs = [Song(**item) for item in data.get('songs', []) if isinstance(item, dict)]
                return setlist_songs, songs
        except:
            pass
        
        return [], []
    
    def _parse_cultures(self, response: str, concert_title: str) -> List[Culture]:
        try:
            json_str = self._extract_json_from_response(response, '[', ']')
            if json_str:
                data = json.loads(json_str)
                return [Culture(**item) for item in data if isinstance(item, dict)]
        except:
            pass
        
        return []
    
    def _parse_schedules(self, response: str, concert_title: str) -> List[Schedule]:
        try:
            json_str = self._extract_json_from_response(response, '[', ']')
            if json_str:
                data = json.loads(json_str)
                return [Schedule(**item) for item in data if isinstance(item, dict)]
        except:
            pass
        
        return []
    
    def _parse_merchandise(self, response: str, concert_title: str) -> List[Merchandise]:
        try:
            json_str = self._extract_json_from_response(response, '[', ']')
            if json_str:
                data = json.loads(json_str)
                return [Merchandise(**item) for item in data if isinstance(item, dict)]
        except:
            pass
        
        return []
    
    def _parse_concert_info(self, response: str, concert_title: str) -> List[ConcertInfo]:
        try:
            json_str = self._extract_json_from_response(response, '[', ']')
            if json_str:
                data = json.loads(json_str)
                return [ConcertInfo(**item) for item in data if isinstance(item, dict)]
        except:
            pass
        
        return []
    
    def _parse_artist_info(self, response: str, artist_name: str) -> Optional[Artist]:
        try:
            json_str = self._extract_json_from_response(response, '{', '}')
            if json_str:
                data = json.loads(json_str)
                return Artist(**data)
        except:
            pass
        
        return Artist(
            artist=artist_name,
            birth_date="",
            birth_place="",
            category="",
            detail="",
            instagram_url="",
            keywords="",
            img_url=""
        )
    
    def _map_status_to_string(self, status: str) -> str:
        """KOPIS 상태 코드를 문자열로 매핑"""
        status_mapping = {
            '01': '공연예정',    # 공연예정
            '02': '공연중',      # 공연중
            '03': '공연완료'     # 공연완료
        }
        return status_mapping.get(status, '알 수 없음')
