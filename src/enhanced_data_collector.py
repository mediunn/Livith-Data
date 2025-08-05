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
        
        # 아티스트 정보 보완 (KOPIS 데이터가 비어있거나 부족한 경우)
        final_artist_name = self._ensure_artist_name(concert_title, artist_name)
        
        # 아티스트 표기용 이름 수집
        artist_display = self._collect_artist_display_name(concert_title, final_artist_name)
        
        # 티켓 URL 수집
        ticket_url = self._collect_ticket_url(concert_title, final_artist_name)
        
        # 날짜 기반 상태 결정
        status = self._determine_status_from_dates(
            self._format_date(kopis_concert['start_date']),
            self._format_date(kopis_concert['end_date'])
        )
        
        # 기본 콘서트 정보 생성 - KOPIS 데이터를 데이터 모델에 맞게 매핑
        concert = Concert(
            title=concert_title,
            start_date=self._format_date(kopis_concert['start_date']),
            end_date=self._format_date(kopis_concert['end_date']),
            artist=final_artist_name,  # 매칭용 (보완된 아티스트명)
            artist_display=artist_display,  # 표기용
            poster=kopis_concert.get('poster', ''),
            status=status,
            venue=kopis_concert.get('venue', ''),
            ticket_url=ticket_url,  # 퍼플렉시티로 수집
            sorted_index=0  # 나중에 계산
        )
        
        # 상세 데이터 수집 (보완된 아티스트명 사용)
        setlists = self._collect_setlists(concert_title, final_artist_name)
        concert_setlists = self._collect_concert_setlists(concert_title, setlists)
        setlist_songs, songs = self._collect_songs_data(setlists, final_artist_name)
        cultures = self._collect_cultures(concert_title, final_artist_name)
        schedules = self._collect_schedules(concert_title, final_artist_name, concert.start_date, concert.end_date)
        merchandise = self._collect_merchandise(concert_title, final_artist_name)
        concert_info = self._collect_concert_info(concert_title, final_artist_name)
        artist_info = self._collect_artist_info(final_artist_name)
        
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
    
    @staticmethod
    def calculate_sorted_indices(concerts: List[Concert]) -> List[Concert]:
        """콘서트 목록에 sorted_index 계산하여 적용"""
        # 상태별로 분류
        ongoing = [c for c in concerts if c.status == "ONGOING"]
        upcoming = [c for c in concerts if c.status == "UPCOMING"]
        past = [c for c in concerts if c.status == "PAST"]
        
        # 각 그룹 내에서 정렬
        # ONGOING: 시작일 기준 오름차순 (빠른 날짜 먼저)
        ongoing.sort(key=lambda x: x.start_date)
        
        # UPCOMING: 시작일 기준 오름차순 (가까운 날짜 먼저)
        upcoming.sort(key=lambda x: x.start_date)
        
        # PAST: 시작일 기준 내림차순 (최근 날짜 먼저)
        past.sort(key=lambda x: x.start_date, reverse=True)
        
        # sorted_index 할당
        index = 1
        for concert in ongoing + upcoming + past:
            concert.sorted_index = index
            index += 1
        
        return ongoing + upcoming + past
    
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
        prompt = f"""{artist_name}의 "{concert_title}" 콘서트만의 독특하고 고유한 문화적 특징을 검색해주세요.

다음과 같은 고유한 특징들을 우선적으로 찾아주세요:
- 이 아티스트만의 특별한 응원 방법이나 팬 문화 (특정 구호, 손동작, 응원 도구 등)
- 이 아티스트 콘서트에서만 볼 수 있는 독특한 순간이나 전통
- 팬들이 특별히 준비하는 이 공연만의 복장이나 아이템
- 이 아티스트와 팬 사이의 특별한 소통 방식이나 약속
- 공연 장르나 스타일로 인한 독특한 관람 문화
- 해당 공연장에서만 경험할 수 있는 특별한 분위기나 특징
- 이 공연에서 금지되거나 권장되는 특별한 행동들
- 팬들 사이에서 전해지는 이 공연만의 숨겨진 팁이나 관례

일반적인 티켓팅 정보나 기본 공연장 정보는 제외하고, 오직 이 공연만의 고유하고 특별한 문화적 요소만 찾아주세요.

응답 작성 규칙:
- 말투는 반드시 해요체로 통일해주세요 (예: "~이에요", "~해요", "~돼요")
- 출처나 참조 표시는 절대 포함하지 마세요 ([출처:], [1], [2], URL 등 제외)
- "정보를 찾을 수 없습니다"라고 답하지 말고, 비슷한 장르나 아티스트의 일반적인 문화라도 유추해서 제공해주세요
- 구체적이고 흥미로운 정보만 포함해주세요

JSON 형식으로만 답변:
[{{"concert_title": "{concert_title}", "title": "고유 문화 특징 제목", "content": "구체적이고 흥미로운 해요체 설명"}}]

JSON 배열만 반환하세요."""
        
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
        prompt = f""""{artist_name}" 아티스트에 대한 정보를 정확하게 검색해주세요.

검색할 아티스트: {artist_name}

반드시 위에 명시된 "{artist_name}" 아티스트의 정보만 찾아주세요. 다른 아티스트의 정보는 절대 포함하지 마세요.

JSON 형식으로만 답변:
{{"artist": "{artist_name}", "birth_date": "YYYY-MM-DD 또는 정보를 찾을 수 없습니다", "birth_place": "출생지 또는 정보를 찾을 수 없습니다", "category": "아티스트 카테고리 또는 정보를 찾을 수 없습니다", "detail": "한국어 상세 설명 또는 정보를 찾을 수 없습니다", "instagram_url": "인스타그램URL 또는 정보를 찾을 수 없습니다", "keywords": "키워드1,키워드2,키워드3 또는 정보를 찾을 수 없습니다", "img_url": "프로필이미지URL 또는 정보를 찾을 수 없습니다"}}

JSON만 반환하고, 반드시 "{artist_name}" 아티스트의 정보만 제공하세요."""
        
        response = self.api.query_with_search(prompt)
        return self._parse_artist_info(response, artist_name)
    
    def _ensure_artist_name(self, concert_title: str, original_artist: str) -> str:
        """아티스트 정보가 비어있거나 부족한 경우 콘서트 제목에서 추출"""
        # 기존 아티스트 정보가 충분한 경우 그대로 사용
        if original_artist and original_artist.strip() and len(original_artist.strip()) > 1:
            return original_artist.strip()
        
        # 콘서트 제목에서 아티스트 정보 추출
        extracted_artist = self._extract_artist_from_title(concert_title)
        
        if extracted_artist:
            logger.info(f"콘서트 제목에서 아티스트 추출: '{concert_title}' -> '{extracted_artist}'")
            return extracted_artist
        
        # 퍼플렉시티로 아티스트 정보 검색
        searched_artist = self._search_artist_from_concert(concert_title)
        
        if searched_artist:
            logger.info(f"퍼플렉시티 검색으로 아티스트 발견: '{concert_title}' -> '{searched_artist}'")
            return searched_artist
        
        # 모든 방법이 실패한 경우 콘서트 제목을 기반으로 추정
        fallback_artist = self._generate_fallback_artist(concert_title)
        logger.warning(f"아티스트 정보 추출 실패, 추정값 사용: '{concert_title}' -> '{fallback_artist}'")
        return fallback_artist

    def _extract_artist_from_title(self, concert_title: str) -> Optional[str]:
        """콘서트 제목에서 아티스트명 추출"""
        import re
        
        # 패턴 1: "아티스트명 내한공연" 형태
        pattern1 = r'^(.+?)\s*내한공연'
        match = re.search(pattern1, concert_title)
        if match:
            return match.group(1).strip()
        
        # 패턴 2: "아티스트명 ASIA TOUR" 형태
        pattern2 = r'^(.+?)\s*ASIA\s*TOUR'
        match = re.search(pattern2, concert_title, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        
        # 패턴 3: "아티스트명 Live in Seoul/Korea" 형태
        pattern3 = r'^(.+?)\s*Live\s+in\s+(Seoul|Korea)'
        match = re.search(pattern3, concert_title, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        
        # 패턴 4: "아티스트명 Tour [도시]" 형태
        pattern4 = r'^(.+?)\s*Tour\s*\[[^\]]+\]'
        match = re.search(pattern4, concert_title, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        
        # 패턴 5: "아티스트명 [도시]" 형태 (단순 형태)
        pattern5 = r'^(.+?)\s*\[(서울|Seoul|부산|대구|인천|광주|대전|울산|수원|고양|용인|성남|청주|전주|천안|안산|안양|부천|평택|시흥|김포|의정부|춘천|원주|강릉|제주)[^\]]*\]'
        match = re.search(pattern5, concert_title)
        if match:
            return match.group(1).strip()
        
        return None

    def _search_artist_from_concert(self, concert_title: str) -> Optional[str]:
        """퍼플렉시티 API로 콘서트 제목을 통해 아티스트 검색"""
        prompt = f""""{concert_title}" 콘서트의 주요 출연 아티스트가 누구인지 검색해주세요.

콘서트 제목: {concert_title}

다음 중 하나로 답변해주세요:
1. 명확한 아티스트명이 있다면 그 이름
2. 여러 아티스트가 있다면 헤드라이너나 주요 아티스트
3. 정보를 찾을 수 없다면 "정보를 찾을 수 없습니다"

JSON 형식으로만 답변:
{{"artist": "아티스트명 또는 정보를 찾을 수 없습니다"}}

JSON만 반환하세요."""
        
        try:
            response = self.api.query_with_search(prompt)
            json_str = self._extract_json_from_response(response, '{', '}')
            if json_str:
                data = json.loads(json_str)
                artist = data.get('artist', '').strip()
                if artist and artist != '정보를 찾을 수 없습니다' and len(artist) > 1:
                    return artist
        except Exception as e:
            logger.error(f"퍼플렉시티 아티스트 검색 실패: {e}")
        
        return None

    def _generate_fallback_artist(self, concert_title: str) -> str:
        """콘서트 제목을 기반으로 추정 아티스트명 생성"""
        import re
        
        # 불필요한 키워드 제거
        clean_title = concert_title
        remove_keywords = [
            r'\s*내한공연.*$',
            r'\s*ASIA\s*TOUR.*$',
            r'\s*Live\s+in\s+(Seoul|Korea).*$',
            r'\s*Tour\s*\[[^\]]+\].*$',
            r'\s*\[(서울|Seoul|부산|대구|인천|광주|대전|울산|수원|고양|용인|성남|청주|전주|천안|안산|안양|부천|평택|시흥|김포|의정부|춘천|원주|강릉|제주)[^\]]*\].*$',
            r'\s*콘서트.*$',
            r'\s*공연.*$',
            r'\s*with.*$',
            r'\s*featuring.*$',
            r'\s*ft\..*$'
        ]
        
        for pattern in remove_keywords:
            clean_title = re.sub(pattern, '', clean_title, flags=re.IGNORECASE)
        
        # 추가 정리
        clean_title = clean_title.strip()
        clean_title = re.sub(r'\s+', ' ', clean_title)  # 연속 공백 제거
        
        # 너무 길면 첫 번째 단어나 구문만 사용
        if len(clean_title) > 50:
            words = clean_title.split()
            if len(words) > 3:
                clean_title = ' '.join(words[:3])
        
        return clean_title if clean_title else "알 수 없는 아티스트"

    def _collect_artist_display_name(self, concert_title: str, artist_name: str) -> str:
        """퍼플렉시티 API로 아티스트 표기용 이름 수집"""
        prompt = f""""{concert_title}" 콘서트의 아티스트 정보를 검색해서 정확한 표기명을 찾아주세요.

현재 아티스트명: {artist_name}

다음 규칙에 따라 표기용 아티스트명을 찾아주세요:
- 한국어 및 원어 표기가 모두 있는 경우: "한국어표기/원어표기" 형식 (예: "아이유/IU")
- 원어만 있는 경우: 원어 그대로 (예: "Hitorie")
- 한국어만 있는 경우: 한국어 그대로 (예: "녹황색사회")
- 복수 아티스트인 경우: 대표 아티스트명 위주로 (예: "레미 파노시앙 트리오")

JSON 형식으로만 답변:
{{"artist_display": "정확한 표기용 아티스트명"}}

JSON만 반환하세요."""
        
        try:
            response = self.api.query_with_search(prompt)
            json_str = self._extract_json_from_response(response, '{', '}')
            if json_str:
                data = json.loads(json_str)
                display_name = data.get('artist_display', '').strip()
                if display_name and display_name != '정보를 찾을 수 없습니다':
                    return display_name
        except Exception as e:
            logger.error(f"아티스트 표기명 수집 실패: {e}")
        
        # 퍼플렉시티 실패시 기본 표기명 생성
        return self._generate_display_name_fallback(artist_name)

    def _generate_display_name_fallback(self, artist_name: str) -> str:
        """아티스트 표기명 생성 실패시 대체 로직"""
        if not artist_name or artist_name.strip() == '':
            return "알 수 없는 아티스트"
        
        # 기본적으로 아티스트명 그대로 사용하되, 정리만 수행
        clean_name = artist_name.strip()
        
        # 과도하게 긴 경우 축약
        if len(clean_name) > 100:
            clean_name = clean_name[:97] + "..."
        
        return clean_name

    def _collect_ticket_url(self, concert_title: str, artist_name: str) -> str:
        """퍼플렉시티 API로 티켓 예매 URL 수집"""
        prompt = f""""{artist_name}"의 "{concert_title}" 콘서트 티켓 예매 URL을 검색해주세요.

콘서트 정보:
- 제목: {concert_title}
- 아티스트: {artist_name}

다음 우선순위로 티켓 예매 URL을 찾아주세요:
1. 인터파크 티켓 (ticket.interpark.com)
2. 예스24 tiket (ticket.yes24.com)
3. 멜론티켓 (ticket.melon.com)
4. 티켓링크 (www.ticketlink.co.kr)
5. 기타 공식 예매처

찾을 수 없는 경우 "정보를 찾을 수 없습니다"로 답변해주세요.

JSON 형식으로만 답변:
{{"ticket_url": "예매 URL 또는 정보를 찾을 수 없습니다"}}

JSON만 반환하세요."""
        
        try:
            response = self.api.query_with_search(prompt)
            json_str = self._extract_json_from_response(response, '{', '}')
            if json_str:
                data = json.loads(json_str)
                url = data.get('ticket_url', '').strip()
                if url and url != '정보를 찾을 수 없습니다' and url.startswith('http'):
                    return url
        except Exception as e:
            logger.error(f"티켓 URL 수집 실패: {e}")
        
        return ""  # 실패시 빈 문자열

    def _determine_status_from_dates(self, start_date: str, end_date: str) -> str:
        """날짜를 기반으로 콘서트 상태 결정"""
        try:
            today = datetime.now().date()
            start = datetime.strptime(start_date, "%Y-%m-%d").date()
            end = datetime.strptime(end_date, "%Y-%m-%d").date()
            
            if today < start:
                return "UPCOMING"
            elif start <= today <= end:
                return "ONGOING"
            else:
                return "PAST"
        except Exception as e:
            logger.error(f"날짜 상태 결정 실패: {e}")
            return "PAST"  # 기본값

    def _format_date(self, date_str: str) -> str:
        """날짜 형식 변환 (YYYY.MM.DD -> YYYY-MM-DD)"""
        if not date_str:
            return "2025-01-01"
        
        try:
            # YYYY.MM.DD 형식인 경우
            if '.' in date_str:
                parts = date_str.split('.')
                if len(parts) == 3:
                    return f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
            
            # YYYYMMDD 형식인 경우 (기존 로직 유지)
            if len(date_str) == 8 and date_str.isdigit():
                return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            
            # 이미 YYYY-MM-DD 형식인 경우
            if '-' in date_str and len(date_str) == 10:
                return date_str
                
        except Exception as e:
            logger.error(f"날짜 형식 변환 실패: {date_str} - {e}")
        
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
                cultures = []
                for item in data:
                    if isinstance(item, dict):
                        title = item.get('title', '').strip()
                        content = item.get('content', item.get('description', '')).strip()
                        
                        # "정보를 찾을 수 없습니다" 관련 내용 필터링
                        skip_keywords = [
                            "정보를 찾을 수 없습니다", "찾을 수 없습니다", "확인할 수 없습니다",
                            "검색 결과에서 확인되지 않았습니다", "공식적으로 공개된 내용을 찾을 수 없습니다",
                            "구체적인 정보는 공식 채널에 명시되어 있지 않습니다"
                        ]
                        
                        # 유효하지 않은 내용이면 건너뛰기
                        if not title or not content or any(keyword in content for keyword in skip_keywords):
                            continue
                        
                        # 출처 표시 제거
                        content = self._remove_sources(content)
                        
                        # 말투 통일 (해요체)
                        content = self._normalize_tone(content)
                        
                        culture_data = {
                            'concert_title': item.get('concert_title', concert_title),
                            'title': title,
                            'content': content
                        }
                        cultures.append(Culture(**culture_data))
                return cultures
        except Exception as e:
            logger.error(f"문화 정보 파싱 실패: {e}")
        
        # JSON 파싱 실패시 응답에서 직접 정보 추출 시도
        if response and len(response.strip()) > 20:
            # "정보를 찾을 수 없습니다" 포함 여부 확인
            skip_keywords = ["정보를 찾을 수 없습니다", "찾을 수 없습니다", "확인할 수 없습니다"]
            if not any(keyword in response for keyword in skip_keywords):
                logger.info("JSON 파싱 실패, 텍스트에서 정보 추출 시도")
                content = self._remove_sources(response[:500])
                content = self._normalize_tone(content)
                return [Culture(
                    concert_title=concert_title,
                    title="콘서트 관련 정보",
                    content=content + "..." if len(response) > 500 else content
                )]
        
        # 완전히 실패한 경우 해당 아티스트나 장르의 추정 문화 정보 제공
        if "indie" in artist_name.lower() or "웹스터" in artist_name or "indie" in concert_title.lower():
            return [
                Culture(
                    concert_title=concert_title,
                    title="인디 콘서트 특유의 친밀한 분위기",
                    content="인디 아티스트들의 콘서트는 대형 공연장보다는 소규모 라이브하우스에서 열리는 경우가 많아, 아티스트와 관객 간의 거리가 가깝습니다. 공연 중 아티스트가 직접 관객과 대화하는 시간이 많고, 편안하고 자유로운 분위기에서 진행됩니다."
                ),
                Culture(
                    concert_title=concert_title,
                    title="조용한 감상 문화",
                    content="인디/얼터너티브 장르 특성상 서정적인 곡들이 많아, 팬들은 조용히 음악에 집중하며 감상하는 문화가 발달되어 있습니다. 큰 소리로 떼창하기보다는 가사에 집중하고, 아티스트의 감정을 함께 느끼는 것을 중요하게 생각합니다."
                )
            ]
        elif "jazz" in artist_name.lower() or "jazz" in concert_title.lower() or "알 디 메올라" in artist_name:
            return [
                Culture(
                    concert_title=concert_title,
                    title="재즈 공연의 즉흥연주 감상법",
                    content="재즈 콘서트에서는 즉흥연주(improvisation)가 중요한 부분을 차지합니다. 관객들은 연주자의 기교적인 솔로 연주 후 박수를 치는 것이 관례이며, 특히 뛰어난 연주에는 '브라보'나 휘파람으로 감탄을 표현하기도 합니다."
                ),
                Culture(
                    concert_title=concert_title,
                    title="앉아서 감상하는 문화",
                    content="재즈 공연은 음악의 섬세함과 복잡함을 집중해서 들어야 하기 때문에, 대부분 앉아서 조용히 감상하는 것이 일반적입니다. 휴대폰 사용을 자제하고, 연주 중에는 대화를 피하는 것이 매너입니다."
                )
            ]
        else:
            return [
                Culture(
                    concert_title=concert_title,
                    title="이 공연만의 특별한 순간",
                    content="모든 라이브 공연에는 그 순간에만 경험할 수 있는 특별함이 있습니다. 아티스트와 관객이 함께 만들어가는 유일무이한 경험을 통해 음악의 진정한 매력을 느낄 수 있습니다."
                )
            ]
    
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
    
    def _map_kopis_status_to_string(self, status: str) -> str:
        """KOPIS 상태를 한국어로 매핑 (참고용)"""
        status_mapping = {
            '01': '공연예정',    # 공연예정
            '02': '공연중',      # 공연중
            '03': '공연완료',    # 공연완료
            '공연예정': '공연예정',
            '공연중': '공연중', 
            '공연완료': '공연완료'
        }
        return status_mapping.get(status, '알 수 없음')
    
    def _remove_sources(self, text: str) -> str:
        """출처 표시 제거"""
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
    
    def _normalize_tone(self, text: str) -> str:
        """말투를 해요체로 통일"""
        if not text:
            return text
        
        import re
        
        # 문장 끝 패턴들을 해요체로 변경
        replacements = [
            (r'입니다\.', '이에요.'),
            (r'됩니다\.', '돼요.'),
            (r'습니다\.', '어요.'),
            (r'다\.', '어요.'),
            (r'한다\.', '해요.'),
            (r'이다\.', '이에요.'),
            (r'있다\.', '있어요.'),
            (r'없다\.', '없어요.'),
            (r'합니다\.', '해요.'),
            (r'받습니다\.', '받아요.'),
            (r'갑니다\.', '가요.'),
            (r'옵니다\.', '와요.'),
        ]
        
        normalized_text = text
        for old_pattern, new_pattern in replacements:
            normalized_text = re.sub(old_pattern, new_pattern, normalized_text)
        
        return normalized_text
