import json
import time
import logging
import re
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
from src.perplexity_api import PerplexityAPI
from data_processing.data_models import *
from src.artist_name_mapper import ArtistNameMapper
from utils.config import Config
from utils.prompts import DataCollectionPrompts

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
        
        # 아티스트 표기용 이름 수집 (KOPIS 아티스트 정보 전달)
        artist_display = self._collect_artist_display_name(concert_title, final_artist_name, artist_name)
        
        # 티켓 URL 및 사이트 정보 수집
        ticket_info = self._collect_ticket_info(concert_title, final_artist_name)
        
        # 날짜 기반 상태 결정
        status = self._determine_status_from_dates(
            self._format_date(kopis_concert['start_date']),
            self._format_date(kopis_concert['end_date'])
        )
        
        # 기본 콘서트 정보 생성 - KOPIS 데이터를 데이터 모델에 맞게 매핑
        concert = Concert(
            artist=artist_display,  # 표기용 아티스트명 (기존 artist_display)
            code=kopis_concert.get('code', ''),  # KOPIS 공연 코드
            title=concert_title,
            start_date=self._format_date(kopis_concert['start_date']),
            end_date=self._format_date(kopis_concert['end_date']),
            status=status,
            poster=kopis_concert.get('poster', ''),
            sorted_index=0,  # 나중에 계산
            ticket_site=ticket_info.get('site', ''),
            ticket_url=ticket_info.get('url', ''),
            venue=kopis_concert.get('venue', '')
        )
        
        # 상세 데이터 수집 (보완된 아티스트명 사용)
        # 셋리스트 생성 (공연 상태에 따라)
        setlists = self._collect_setlists(concert_title, final_artist_name, status)
        
        # 셋리스트에 날짜와 장소 정보 추가
        for setlist in setlists:
            setlist.start_date = concert.start_date
            setlist.end_date = concert.end_date
            setlist.venue = concert.venue
        
        # 콘서트-셋리스트 연결 정보 생성
        concert_setlists = []
        for setlist in setlists:
            if "예상 셋리스트" in setlist.title:
                concert_setlists.append(ConcertSetlist(
                    concert_title=concert_title,
                    setlist_title=setlist.title,
                    type="EXPECTED",
                    status=""
                ))
            else:
                concert_setlists.append(ConcertSetlist(
                    concert_title=concert_title,
                    setlist_title=setlist.title,
                    type="PAST",
                    status=""
                ))
        
        # 곡 정보 수집
        logger.info(f"곡 정보 수집 시작: {final_artist_name}")
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
    
    def _collect_setlists(self, concert_title: str, artist_name: str, concert_status: str) -> List[Setlist]:
        """
        공연 상태에 따른 셋리스트 정보 수집
        - UPCOMING: 예상 셋리스트만 생성
        - PAST: 실제 과거 셋리스트만 생성  
        - ONGOING: 예상 셋리스트만 생성 (아직 완료되지 않았으므로)
        """
        setlists = []
        
        if concert_status in ["UPCOMING", "ONGOING"]:
            # 예정/진행 중인 공연 → 예상 셋리스트만 생성
            setlists.append(Setlist(
                title=f"{concert_title} 예상 셋리스트",
                start_date="",  # 나중에 콘서트 정보에서 채움
                end_date="",    # 나중에 콘서트 정보에서 채움
                img_url="",
                artist=artist_name,
                venue=""
            ))
            logger.info(f"공연 상태 '{concert_status}' → 예상 셋리스트만 생성")
            
        elif concert_status == "PAST":
            # 완료된 공연 → 실제 셋리스트만 생성
            setlists.append(Setlist(
                title=f"{concert_title} 실제 셋리스트",
                start_date="",  # 나중에 콘서트 정보에서 채움
                end_date="",    # 나중에 콘서트 정보에서 채움  
                img_url="",
                artist=artist_name,
                venue=""
            ))
            logger.info(f"공연 상태 '{concert_status}' → 실제 셋리스트만 생성")
            
        else:
            # 알 수 없는 상태 → 예상 셋리스트 기본 생성
            setlists.append(Setlist(
                title=f"{concert_title} 예상 셋리스트",
                start_date="",
                end_date="",
                img_url="",
                artist=artist_name,
                venue=""
            ))
            logger.warning(f"알 수 없는 공연 상태 '{concert_status}' → 예상 셋리스트 기본 생성")
            
        return setlists
        
    
    def _collect_concert_setlists(self, concert_title: str, setlists: List[Setlist]) -> List[ConcertSetlist]:
        """콘서트-셋리스트 연결 정보 수집 - 이제 collect_concert_data에서 직접 생성"""
        # 이 함수는 더 이상 사용되지 않음 (collect_concert_data에서 직접 처리)
        return []
    
    def _collect_songs_data(self, setlists: List[Setlist], artist_name: str) -> Tuple[List[SetlistSong], List[Song]]:
        """곡 정보 수집 - 예상 셋리스트와 예전 셋리스트 모두 수집"""
        all_setlist_songs = []
        all_songs = []
        valid_setlists = []  # 유효한 셋리스트만 저장
        
        for setlist in setlists:
            # 예상 셋리스트인지 과거 셋리스트인지 확인
            if "예상 셋리스트" in setlist.title:
                # 예상 셋리스트 수집 - 무조건 15곡 이상 생성
                prompt = f"""🚨 중요: {artist_name}의 콘서트 예상 셋리스트를 15곡 이상 반드시 만들어주세요! 🚨

다음을 기반으로 정확히 15-20곡을 작성해야 합니다:
1. {artist_name}의 대표 히트곡 5곡 이상
2. 최신/인기 앨범 수록곡 3곡 이상  
3. 팬들이 가장 사랑하는 곡 3곡 이상
4. 콘서트 정규 레퍼토리 4곡 이상

⚠️ 절대 준수 사항:
- 곡 개수는 최소 15개, 최대 20개 (이 범위를 벗어나면 안 됩니다!)
- song_title과 title 필드는 절대 빈 문자열이면 안 됩니다
- 모든 곡은 {artist_name}의 실제 곡이어야 합니다
- order_index는 1부터 순서대로 매기세요

JSON 응답 형식 (정확히 이 구조로):
{{"setlist_songs": [{{"setlist_title": "{setlist.title}", "song_title": "곡제목1", "setlist_date": "{setlist.start_date}", "order_index": 1, "fanchant": "", "venue": "{setlist.venue}"}}, {{"setlist_title": "{setlist.title}", "song_title": "곡제목2", "setlist_date": "{setlist.start_date}", "order_index": 2, "fanchant": "", "venue": "{setlist.venue}"}}, ... (15-20개까지)], "songs": [{{"title": "곡제목1", "artist": "{artist_name}", "lyrics": "", "pronunciation": "", "translation": "", "youtube_id": ""}}, {{"title": "곡제목2", "artist": "{artist_name}", "lyrics": "", "pronunciation": "", "translation": "", "youtube_id": ""}}, ... (15-20개까지)]}}

15곡 미만으로 응답하면 오류입니다! JSON만 반환하세요."""
            elif "실제 셋리스트" in setlist.title:
                # 실제 셋리스트 수집 - 완료된 공연의 실제 연주곡 검색
                search_artist = ArtistNameMapper.get_optimal_search_name(artist_name)
                korean_name, english_name = ArtistNameMapper.get_search_names(artist_name)
                
                # 검색에 사용할 모든 이름들
                search_terms = []
                if english_name:
                    search_terms.append(f'"{english_name}"')
                if korean_name and korean_name != english_name:
                    search_terms.append(f'"{korean_name}"')
                if search_artist not in [english_name, korean_name]:
                    search_terms.append(f'"{search_artist}"')
                
                search_terms_str = " OR ".join(search_terms) if search_terms else f'"{artist_name}"'
                
                prompt = f"""다음 아티스트의 콘서트에서 실제로 연주한 셋리스트를 전 세계적으로 검색해주세요.

아티스트 정보:
- 원어명: {english_name if english_name else "없음"}  
- 최적 검색명: {search_artist}

검색 키워드: {search_terms_str}

검색 대상 (우선순위):
1. setlist.fm - {search_artist} 공연 기록 (전 세계 어디든)
2. 해외 음악 사이트 - {search_artist} recent concert setlists 
3. 해외 콘서트 리뷰 - {search_artist} live performance reviews
4. 팬 사이트 - {search_artist} tour setlists worldwide
5. 유튜브 콘서트 영상 - {search_artist} live concert full show
6. 음악 매거진 - {search_artist} concert reviews and setlists

추가 검색 키워드 (모두 시도):
- "{search_artist} setlist 2024"
- "{search_artist} concert setlist recent"  
- "{search_artist} tour songs list"
- "{search_artist} live performance tracklist"
- "{english_name} setlist" (영어명이 있는 경우)
- "{korean_name} 셋리스트" (한국어명이 있는 경우)

중요 규칙:
- 한국 공연에 국한하지 말고 전 세계 최신 공연 기록을 우선 검색하세요
- setlist.fm은 가장 신뢰할 수 있는 출처이므로 우선적으로 활용하세요
- 여러 언어로 검색하여 더 많은 정보를 찾으세요
- 실제 공연에서 연주된 곡 목록을 찾지 못하면 {search_artist}의 히트곡과 대표곡으로 구성하세요
- 최소 10곡 이상 포함해주세요
- 모든 song_title 필드에 실제 곡 제목을 반드시 넣어주세요
- song_title이 비어있으면 안 되며, 찾지 못했을 경우 데이터를 추가하지 마세요.

JSON 형식으로만 답변:
{{"setlist_songs": [{{"setlist_title": "{setlist.title}", "song_title": "실제 곡 제목 (비워두지 마세요)", "setlist_date": "{setlist.start_date}", "order_index": 1, "fanchant": "", "venue": "{setlist.venue}"}}], "songs": [{{"title": "실제 곡 제목 (비워두지 마세요)", "artist": "{artist_name}", "lyrics": "", "pronunciation": "", "translation": "", "youtube_id": ""}}]}}

JSON만 반환하세요."""
            
            # 셋리스트 수집
            logger.info(f"셋리스트 수집 중: {setlist.title}")
            
            # 예상 셋리스트는 최대 3번 재시도
            max_retries = 3 if "예상 셋리스트" in setlist.title else 1
            setlist_songs, songs = [], []
            
            for attempt in range(max_retries):
                response = self.api.query_with_search(prompt, context="셋리스트 수집")
                setlist_songs, songs = self._parse_and_validate_songs(response, setlist, artist_name)
                
                # 예상 셋리스트는 10곡 이상일 때 성공으로 간주
                if "예상 셋리스트" in setlist.title:
                    if len(songs) >= 10:
                        logger.info(f"예상 셋리스트 {len(songs)}곡 수집 성공 (시도 {attempt + 1}/{max_retries})")
                        break
                    elif attempt < max_retries - 1:
                        logger.warning(f"예상 셋리스트 {len(songs)}곡만 생성됨, 재시도 {attempt + 2}/{max_retries}")
                        time.sleep(2)  # 재시도 전 잠시 대기
                    else:
                        logger.error(f"예상 셋리스트 {len(songs)}곡으로 최종 확정 (재시도 완료)")
                else:
                    # 과거 셋리스트는 첫 시도만
                    break
            
            # 셋리스트 유형에 따른 처리
            if "예상 셋리스트" in setlist.title:
                # 예상 셋리스트는 곡이 적어도 항상 포함
                all_setlist_songs.extend(setlist_songs)
                all_songs.extend(songs)
                valid_setlists.append(setlist)
                if len(songs) >= 15:
                    logger.info(f"✅ 예상 셋리스트 {len(songs)}곡 수집 완료 (목표 달성)")
                elif len(songs) >= 10:
                    logger.warning(f"⚠️ 예상 셋리스트 {len(songs)}곡 수집 완료 (목표 미달성이지만 허용)")
                else:
                    logger.error(f"❌ 예상 셋리스트 {len(songs)}곡만 수집됨 (목표 크게 미달성)")
            elif "실제 셋리스트" in setlist.title:
                # 실제 셋리스트는 10곡 이상일 때만 추가
                if len(songs) >= 10:
                    all_setlist_songs.extend(setlist_songs)
                    all_songs.extend(songs)
                    valid_setlists.append(setlist)
                    logger.info(f"✅ 실제 셋리스트 {len(songs)}곡 수집 완료")
                else:
                    logger.warning(f"실제 셋리스트 곡이 10개 미만 ({len(songs)}개), 제외")
            else:
                # 기타 (호환성용)
                if len(songs) >= 10:
                    all_setlist_songs.extend(setlist_songs)
                    all_songs.extend(songs)
                    valid_setlists.append(setlist)
                    logger.info(f"기타 셋리스트 곡 {len(songs)}개 수집 완료")
                else:
                    logger.warning(f"기타 셋리스트 곡이 10개 미만 ({len(songs)}개), 제외")
            
            time.sleep(Config.REQUEST_DELAY)
        
        # 유효한 셋리스트만 반환하도록 setlists 업데이트
        setlists.clear()
        setlists.extend(valid_setlists)
        
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
        
        response = self.api.query_with_search(prompt, context="팬 문화 수집")
        return self._parse_cultures(response, concert_title)
    
    def _collect_schedules(self, concert_title: str, artist_name: str, start_date: str, end_date: str) -> List[Schedule]:
        """스케줄 정보 수집"""
        prompt = f"""{artist_name}의 "{concert_title}" 콘서트 관련 모든 일정을 {start_date}부터 {end_date}까지 검색해주세요.

다음 모든 종류의 일정을 찾아주세요:
1. 티켓팅 관련:
   - 일반예매 시작
   - 팬클럽 선예매 
   - 추가 티켓팅
   - 현장 판매

2. 공연 관련:
   - 공연 시간 (각 회차별로)
   - 입장 시간
   - 리허설 또는 사운드체크

3. 굿즈 관련:
   - 당일 MD 구매 시간
   - 사전 굿즈 판매

4. 기타:
   - 만남의 시간 (팬미팅)
   - 특별 이벤트

공연 일정 카테고리 작성 규칙:
- 하루 공연: "{artist_name} 콘서트"
- 여러 날: "{artist_name} 1일차 콘서트", "{artist_name} 2일차 콘서트"
- 날짜 표시는 빼고 작성하세요

중요:
- scheduled_at 필드는 반드시 채워주세요
- 정확한 시간을 아는 경우: YYYY-MM-DD HH:MM:SS 형식
- 시간을 모르는 경우: YYYY-MM-DD 형식 (날짜만)
- scheduled_at이 비어있으면 그 데이터는 제외됩니다
- 추정하지 말고 실제 정보만 사용하세요

JSON 형식으로만 답변:
[{{"concert_title": "{concert_title}", "category": "일정 카테고리 (예: 티켓팅, {artist_name} 콘서트)", "scheduled_at": "YYYY-MM-DD HH:MM:SS 또는 YYYY-MM-DD (반드시 채우기)"}}]

JSON 배열만 반환하세요."""
        
        response = self.api.query_with_search(prompt)
        return self._parse_schedules(response, concert_title)
    
    def _collect_merchandise(self, concert_title: str, artist_name: str) -> List[Merchandise]:
        """MD 상품 정보 수집"""
        prompt = f""""{artist_name}"의 "{concert_title}" 콘서트 굿즈 판매 현황과 한정판 정보를 검색해주세요:

JSON 형식으로만 답변:
[{{"concert_title": "{concert_title}", "name": "상품명", "price": "원화 가격 (예: 35,000)", "img_url": "상품 이미지 URL"}}]

굿즈 정보를 찾을 수 없는 경우 빈 배열 []로 응답하세요."""
        
        response = self.api.query_with_search(prompt)
        return self._parse_merchandise(response, concert_title)
    
    def _collect_concert_info(self, concert_title: str, artist_name: str) -> List[ConcertInfo]:
        """콘서트 정보 수집"""
        prompt = f"""{artist_name}의 "{concert_title}" 콘서트의 중요한 정보를 검색해주세요.

다음과 같은 실용적인 정보를 찾아주세요:
- 공연장 정보와 좌석 배치
- 공연 관람 규칙과 주의사항
- 입장 및 퇴장 안내
- 주차 및 교통 정보
- 음식물 반입 규정
- 기타 관람객이 알아야 할 정보

중요 규칙:
- content는 반드시 해요체(~해요, ~이에요, ~돼요)로 작성해주세요
- content가 비어있거나 "정보를 찾을 수 없습니다" 같은 내용이면 해당 항목을 아예 포함하지 마세요
- 실제로 유용한 정보가 있는 항목만 반환하세요
- content는 최소 10자 이상의 의미 있는 내용이어야 합니다

JSON 형식으로만 답변:
[{{"concert_title": "{concert_title}", "category": "정보 카테고리", "content": "실제로 유용한 해요체 설명 (10자 이상)", "img_url": "관련 이미지URL 또는 빈문자열"}}]

JSON 배열만 반환하세요."""
        
        response = self.api.query_with_search(prompt)
        return self._parse_concert_info(response, concert_title)
    
    def _collect_artist_info(self, artist_name: str) -> Optional[Artist]:
        """아티스트 정보 수집"""
        prompt = DataCollectionPrompts.get_artist_info_prompt(artist_name)
        
        response = self.api.query_with_search(prompt)
        return self._parse_artist_info(response, artist_name)
    
    def collect_merchandise_data(self, concert: Concert) -> List[Dict[str, str]]:
        """콘서트의 굿즈(merchandise) 정보를 수집합니다."""
        
        prompt = f"""
"{concert.artist}"의 "{concert.title}" 콘서트 공식 굿즈 판매 현황과 한정판 정보를 검색해주세요.

JSON 배열로만 응답 (다른 텍스트 절대 포함 금지):
[
    {{
        "concert_title": "{concert.title}",
        "name": "정확한 상품명 (예: 공식 투어 티셔츠)",
        "price": "정확한 원화 가격 (예: 35,000)",
        "img_url": "실제 상품 이미지 URL"
    }}
]

굿즈를 찾을 수 없으면 빈 배열 []로 응답하세요.
"""
        
        try:
            response = self.api.query_with_search(prompt)
            logger.info(f"굿즈 API 응답: {response[:500]}...")
            
            # JSON 응답을 파싱하여 리스트로 변환
            merchandise_list = self._parse_merchandise_response(response, concert.title)
            
            return merchandise_list
            
        except Exception as e:
            logger.error(f"굿즈 정보 수집 실패 ({concert.title}): {e}")
            return []
    
    def _parse_merchandise_response(self, response: str, concert_title: str) -> List[Dict[str, str]]:
        """굿즈 API 응답을 파싱하여 굿즈 정보 리스트를 반환합니다."""
        try:
            import json
            import re
            
            # 응답 정리
            cleaned_response = self._clean_json_response(response)
            
            # JSON 배열 패턴 찾기 (더 정교한 패턴)
            json_patterns = [
                r'\[[\s\S]*?\]',  # 기본 배열 패턴
                r'\[\s*\{[\s\S]*?\}\s*\]',  # 객체 포함 배열
                r'\[\s*\{[\s\S]*?\}\s*(?:,\s*\{[\s\S]*?\}\s*)*\]'  # 복수 객체 배열
            ]
            
            json_str = None
            for pattern in json_patterns:
                json_match = re.search(pattern, cleaned_response, re.DOTALL)
                if json_match:
                    json_str = json_match.group()
                    break
            
            if json_str:
                try:
                    merchandise_list = json.loads(json_str)
                    
                    # 빈 배열 처리
                    if not merchandise_list:
                        logger.info("굿즈 정보가 없습니다 (빈 배열)")
                        return []
                    
                    # 각 아이템 검증 및 정리
                    required_fields = ['concert_title', 'name', 'price', 'img_url']
                    valid_items = []
                    
                    for item in merchandise_list:
                        if isinstance(item, dict):
                            # concert_title이 없으면 추가
                            if 'concert_title' not in item or not item['concert_title']:
                                item['concert_title'] = concert_title
                            
                            # 필수 필드 검증 및 기본값 설정
                            for field in required_fields:
                                if field not in item or item[field] is None:
                                    item[field] = ""
                            
                            # 상품명과 가격이 있는 경우만 유효한 아이템으로 처리
                            if item['name'].strip() and item['price'].strip():
                                # 가격을 nn,nnn원 형식으로 정리
                                price = item['price'].strip()
                                # 숫자만 추출하고 천 단위 구분자 추가
                                import re
                                numbers = re.findall(r'\d+', price.replace(',', ''))
                                if numbers:
                                    num = int(numbers[0])
                                    formatted_price = f"{num:,}원"
                                    item['price'] = formatted_price
                                
                                valid_items.append(item)
                                logger.debug(f"유효한 굿즈 아이템: {item['name']} - {item['price']}")
                    
                    logger.info(f"총 {len(valid_items)}개의 유효한 굿즈 아이템 파싱 완료")
                    return valid_items
                    
                except json.JSONDecodeError as e:
                    logger.error(f"굿즈 JSON 파싱 실패: {e}")
                    logger.debug(f"파싱 시도한 JSON: {json_str[:200]}...")
            
            # JSON 파싱 실패시 텍스트에서 정보 추출 시도
            logger.warning("JSON 파싱 실패, 텍스트에서 굿즈 정보 추출 시도")
            return self._extract_merchandise_from_text(response, concert_title)
                
        except Exception as e:
            logger.error(f"굿즈 응답 파싱 실패: {e}")
            return []
    
    def _extract_merchandise_from_text(self, response: str, concert_title: str) -> List[Dict[str, str]]:
        """텍스트 응답에서 굿즈 정보 추출"""
        import re
        
        # 굿즈 관련 키워드가 있는지 확인
        merchandise_keywords = ['티셔츠', '후디', '굿즈', 'MD', '포토카드', '키링', '뱃지', '포스터', '앨범']
        if not any(keyword in response for keyword in merchandise_keywords):
            return []
        
        # 간단한 상품 정보 추출 (예시)
        items = []
        
        # 가격 패턴 찾기 (예: 35,000원, 45000원)
        price_patterns = re.findall(r'(\d{1,3}(?:,\d{3})*원|\d+원)', response)
        
        # 상품명 패턴 찾기
        for keyword in merchandise_keywords:
            if keyword in response:
                # 해당 키워드 주변 텍스트에서 가격 찾기
                for price in price_patterns[:3]:  # 최대 3개까지만
                    items.append({
                        'concert_title': concert_title,
                        'name': f"공식 {keyword}",
                        'price': price,
                        'img_url': ""
                    })
                break
        
        return items[:3]  # 최대 3개까지만 반환
    
    def _ensure_artist_name(self, concert_title: str, original_artist: str) -> str:
        """퍼플렉시티 API로 아티스트 정보 검색 후 fallback 로직 적용"""
        # 1순위: 퍼플렉시티로 아티스트 정보 검색
        searched_artist = self._search_artist_from_concert(concert_title)
        
        if searched_artist:
            logger.info(f"퍼플렉시티 검색으로 아티스트 발견: '{concert_title}' -> '{searched_artist}'")
            return searched_artist
        
        # 2순위: 콘서트 제목에서 아티스트 정보 추출 (fallback)
        extracted_artist = self._extract_artist_from_title(concert_title)
        
        if extracted_artist:
            logger.info(f"콘서트 제목에서 아티스트 추출 (fallback): '{concert_title}' -> '{extracted_artist}'")
            return extracted_artist
        
        # 3순위: 콘서트 제목을 기반으로 추정
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
        
        # 패턴 6: "아티스트명 presents 제목" 형태 (예: "MAQIA presents ONEMAN TOUR: Tomoshibi")
        pattern6 = r'^([^:\s]+)\s+presents\s+'
        match = re.search(pattern6, concert_title, re.IGNORECASE)
        if match:
            artist = match.group(1).strip()
            logger.info(f"'presents' 패턴에서 아티스트 추출: '{artist}'")
            return artist
        
        # 패턴 7: "아티스트명: 제목" 형태 (콜론 앞이 아티스트)
        pattern7 = r'^([^:]+?):\s*'
        match = re.search(pattern7, concert_title)
        if match:
            artist = match.group(1).strip()
            # 너무 짧거나 일반적인 제목 키워드가 포함된 경우 제외
            exclude_keywords = ['concert', 'live', 'tour', 'show', 'special', 'presents', 'oneman', '콘서트', '공연', '투어']
            if (len(artist) >= 2 and len(artist) <= 30 and 
                not any(keyword in artist.lower() for keyword in exclude_keywords)):
                logger.info(f"콜론 앞 패턴에서 아티스트 추출: '{artist}'")
                return artist
        
        # 패턴 8: "아티스트명 ONEMAN" 형태 (원맨 공연)
        pattern8 = r'^(.+?)\s+ONEMAN'
        match = re.search(pattern8, concert_title, re.IGNORECASE)
        if match:
            artist = match.group(1).strip()
            logger.info(f"'ONEMAN' 패턴에서 아티스트 추출: '{artist}'")
            return artist
        
        # 패턴 9: 첫 번째 단어가 아티스트명인 경우 (단순 추출)
        first_word_pattern = r'^([A-Za-z가-힣]+(?:[A-Za-z가-힣\s]*[A-Za-z가-힣])?)'
        match = re.search(first_word_pattern, concert_title)
        if match:
            first_part = match.group(1).strip()
            # 너무 짧거나 일반적인 단어는 제외
            exclude_words = ['concert', 'live', 'tour', 'show', 'presents', 'special', '콘서트', '공연']
            if (len(first_part) >= 2 and 
                not any(word in first_part.lower() for word in exclude_words) and
                len(first_part) <= 20):  # 너무 긴 것도 제외
                logger.info(f"첫 번째 단어에서 아티스트 추출: '{first_part}'")
                return first_part
        
        return None

    def _search_artist_from_concert(self, concert_title: str) -> Optional[str]:
        """퍼플렉시티 API로 콘서트 제목을 통해 아티스트 검색"""
        prompt = DataCollectionPrompts.get_artist_name_prompt(concert_title)
        
        try:
            response = self.api.query_with_search(prompt)
            json_str = self._extract_json_from_response(response, '{', '}')
            if json_str:
                data = json.loads(json_str)
                artist = data.get('artist', '').strip()
                if artist and len(artist) > 1:
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

    def _collect_artist_display_name(self, concert_title: str, artist_name: str, kopis_artist: str = "") -> str:
        """퍼플렉시티 API로 아티스트 표기용 이름 수집"""
        
        # KOPIS에서 제공된 아티스트 정보가 있고 충분하다면 그것을 우선 사용
        if kopis_artist and kopis_artist.strip() and len(kopis_artist.strip()) > 1:
            logger.info(f"KOPIS 아티스트 정보 사용: {kopis_artist}")
            return kopis_artist.strip()
        
        # KOPIS 정보가 없거나 불충분한 경우에만 퍼플렉시티로 검색
        prompt = f""""{concert_title}" 콘서트의 아티스트 정보를 검색해서 정확한 표기명을 찾아주세요.

현재 아티스트명: {artist_name}
다음 규칙에 따라 표기용 아티스트명을 찾아주세요: "원어표기 (한국어표기)" 형식

JSON 형식으로만 답변:
{{"artist_display": "정확한 표기용 아티스트명"}}

JSON만 반환하세요."""
        
        try:
            response = self.api.query_with_search(prompt)
            json_str = self._extract_json_from_response(response, '{', '}')
            if json_str:
                data = json.loads(json_str)
                display_name = data.get('artist_display', '').strip()
                if display_name and display_name != "정보를 찾을 수 없습니다":
                    return display_name
        except Exception as e:
            logger.error(f"아티스트 표기명 수집 실패: {e}")
        
        # 퍼플렉시티 실패시 기본 표기명 생성 (KOPIS 정보나 추출된 아티스트명 사용)
        return self._generate_display_name_fallback(artist_name, kopis_artist)

    def _generate_display_name_fallback(self, artist_name: str, kopis_artist: str = "") -> str:
        """아티스트 표기명 생성 실패시 대체 로직"""
        
        # 1순위: KOPIS 아티스트 정보 사용
        if kopis_artist and kopis_artist.strip() and len(kopis_artist.strip()) > 1:
            clean_name = kopis_artist.strip()
            logger.info(f"KOPIS 아티스트 정보를 fallback으로 사용: {clean_name}")
            return clean_name
        
        # 2순위: 추출된 아티스트명 사용
        if artist_name and artist_name.strip():
            clean_name = artist_name.strip()
            
            # 과도하게 긴 경우 축약
            if len(clean_name) > 100:
                clean_name = clean_name[:97] + "..."
            
            return clean_name
        
        # 최후: 기본값
        return "알 수 없는 아티스트"

    def _collect_ticket_info(self, concert_title: str, artist_name: str) -> Dict[str, str]:
        """퍼플렉시티 API로 티켓 예매 정보 수집 (사이트명과 URL)"""
        prompt = f""""{artist_name}"의 "{concert_title}" 콘서트 정확한 예매 링크를 찾아주세요.

콘서트 정보:
- 제목: {concert_title}
- 아티스트: {artist_name}

중요: 티켓 사이트 메인 링크가 아닌, 해당 공연의 구체적인 예매 링크를 찾아주세요.

우선순위:
1. 인터파크 티켓 (ticket.interpark.com) - 구체적인 공연 페이지
2. 예스24 티켓 (ticket.yes24.com) - 구체적인 공연 페이지
3. 멜론티켓 (ticket.melon.com) - 구체적인 공연 페이지
4. 티켓링크 (www.ticketlink.co.kr) - 구체적인 공연 페이지
5. 기타 공식 예매처 - 구체적인 공연 페이지

사이트명은 정확히 다음 중 하나로 답변해주세요:
- "인터파크 티켓"
- "예스24 티켓"
- "멜론티켓"
- "티켓링크"
- "기타 사이트"

예매 링크를 찾을 수 없는 경우 빈 문자열로 답변해주세요.

JSON 형식으로만 답변:
{{"ticket_site": "사이트명 또는 빈문자열", "ticket_url": "해당 공연의 구체적인 예매 URL 또는 빈문자열"}}

JSON만 반환하세요."""
        
        try:
            response = self.api.query_with_search(prompt)
            json_str = self._extract_json_from_response(response, '{', '}')
            if json_str:
                data = json.loads(json_str)
                site = data.get('ticket_site', '').strip()
                url = data.get('ticket_url', '').strip()
                
                # 유효한 정보가 있는 경우만 반환
                if (site and url and url.startswith('http')):
                    return {'site': site, 'url': url}
        except Exception as e:
            logger.error(f"티켓 정보 수집 실패: {e}")
        
        return {'site': '', 'url': ''}  # 실패시 빈 문자열

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
            title=f"{concert_title} 메인 셋리스트",
            start_date="",
            end_date="",
            img_url="",
            artist=artist_name,
            venue=""
        )]
    
    def _parse_concert_setlists(self, response: str, concert_title: str, setlists: List[Setlist]) -> List[ConcertSetlist]:
        try:
            json_str = self._extract_json_from_response(response, '[', ']')
            if json_str:
                data = json.loads(json_str)
                concert_setlists = []
                for item in data:
                    if isinstance(item, dict):
                        # setlist_date 필드가 있으면 제거
                        if 'setlist_date' in item:
                            item.pop('setlist_date')
                        # status를 빈 문자열로 설정
                        item['status'] = ""
                        
                        # setlist_title이 비어있거나 없으면 규칙에 따라 채우기
                        if not item.get('setlist_title'):
                            concert_type = item.get('type', 'PAST')
                            if concert_type == 'EXPECTED':
                                item['setlist_title'] = f"{concert_title} 예상 셋리스트"
                            else:
                                item['setlist_title'] = f"{concert_title} 셋리스트"
                        
                        concert_setlists.append(ConcertSetlist(**item))
                return concert_setlists
        except:
            pass
        
        # 기본값 반환 - setlist_title 규칙에 따라 설정
        default_setlists = []
        for setlist in setlists:
            default_setlists.append(ConcertSetlist(
                concert_title=concert_title,
                setlist_title=f"{concert_title} 셋리스트",
                type="PAST",
                status=""
            ))
        return default_setlists
    
    def _parse_and_validate_songs(self, response: str, setlist: Setlist, artist_name: str) -> Tuple[List[SetlistSong], List[Song]]:
        """곡 데이터를 파싱하고 song_title이 비어있지 않은 것만 반환"""
        try:
            json_str = self._extract_json_from_response(response, '{', '}')
            if json_str:
                data = json.loads(json_str)
                
                # song_title이 있는 것만 필터링
                valid_setlist_songs = []
                for item in data.get('setlist_songs', []):
                    if isinstance(item, dict) and item.get('song_title', '').strip():
                        valid_setlist_songs.append(SetlistSong(**item))
                
                # title이 있는 것만 필터링
                valid_songs = []
                for item in data.get('songs', []):
                    if isinstance(item, dict) and item.get('title', '').strip():
                        valid_songs.append(Song(**item))
                
                return valid_setlist_songs, valid_songs
        except Exception as e:
            logger.error(f"곡 데이터 파싱 실패: {e}")
        
        return [], []
    
    def _parse_songs_data(self, response: str, setlist: Setlist, artist_name: str) -> Tuple[List[SetlistSong], List[Song]]:
        """기존 함수 - 호환성을 위해 유지"""
        return self._parse_and_validate_songs(response, setlist, artist_name)
    
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
                schedules = []
                for item in data:
                    if isinstance(item, dict):
                        # scheduled_at이 비어있지 않은 경우만 추가
                        scheduled_at_value = item.get('scheduled_at', '')
                        if scheduled_at_value and str(scheduled_at_value).strip():
                            schedules.append(Schedule(**item))
                        else:
                            logger.warning(f"scheduled_at이 비어있어 제외: {item.get('category', 'Unknown')}")
                return schedules
        except Exception as e:
            logger.error(f"스케줄 파싱 실패: {e}")
        
        return []
    
    def _parse_merchandise(self, response: str, concert_title: str) -> List[Merchandise]:
        try:
            json_str = self._extract_json_from_response(response, '[', ']')
            if json_str:
                data = json.loads(json_str)
                merchandise_list = []
                for item in data:
                    if isinstance(item, dict):
                        # 가격 형식을 nn,nnn원 형태로 변환
                        if 'price' in item:
                            price = item['price']
                            # 숫자만 추출하고 천 단위 구분자 추가
                            import re
                            numbers = re.findall(r'\d+', price.replace(',', ''))
                            if numbers:
                                num = int(numbers[0])
                                formatted_price = f"{num:,}원"
                                item['price'] = formatted_price
                        merchandise_list.append(Merchandise(**item))
                return merchandise_list
        except:
            pass
        
        return []
    
    def _parse_concert_info(self, response: str, concert_title: str) -> List[ConcertInfo]:
        try:
            json_str = self._extract_json_from_response(response, '[', ']')
            if json_str:
                data = json.loads(json_str)
                concert_infos = []
                for item in data:
                    if isinstance(item, dict):
                        # content가 비어있거나 너무 짧은 경우 해당 항목 제외
                        content = item.get('content', '')
                        category = item.get('category', 'Unknown')
                        
                        # 빈 content나 무의미한 내용 필터링
                        if not content or not content.strip():
                            logger.debug(f"concert_info content가 비어있어 제외: category='{category}'")
                            continue
                            
                        content = content.strip()
                        
                        # 너무 짧거나 무의미한 내용 제외
                        if len(content) < 10:
                            logger.debug(f"concert_info content가 너무 짧아 제외: category='{category}', content='{content[:20]}...'")
                            continue
                            
                        # 무의미한 응답 필터링
                        meaningless_phrases = [
                            "정보를 찾을 수 없습니다",
                            "확인할 수 없습니다", 
                            "알 수 없습니다",
                            "정보가 없습니다",
                            "찾을 수 없습니다",
                            "정보를 제공할 수 없습니다"
                        ]
                        
                        if any(phrase in content for phrase in meaningless_phrases):
                            logger.debug(f"concert_info 무의미한 content로 제외: category='{category}'")
                            continue
                            
                        # content를 해요체로 변환
                        content = self._normalize_tone(content)
                        item['content'] = content
                        concert_infos.append(ConcertInfo(**item))
                return concert_infos
        except:
            pass
        
        return []
    
    def _parse_artist_info(self, response: str, artist_name: str) -> Optional[Artist]:
        try:
            json_str = self._extract_json_from_response(response, '{', '}')
            if json_str:
                data = json.loads(json_str)
                # "정보를 찾을 수 없습니다"를 빈 문자열로 변환
                for key, value in data.items():
                    if isinstance(value, str) and "정보를 찾을 수 없습니다" in value:
                        data[key] = ""
                
                # debut_date는 이미 문자열이므로 특별한 변환 불필요
                debut_date = data.get('debut_date', '')
                if isinstance(debut_date, (int, float)):
                    data['debut_date'] = str(int(debut_date))
                elif not isinstance(debut_date, str):
                    data['debut_date'] = ''
                
                # detail을 해요체로 변환하고 출처 표기 제거
                detail = data.get('detail', '')
                if detail:
                    detail = self._normalize_tone(detail)
                    detail = self._remove_sources(detail)
                    data['detail'] = detail
                
                # keywords에서 아티스트 이름 제거
                keywords = data.get('keywords', '')
                if keywords:
                    keywords = self._filter_artist_name_from_keywords(keywords, artist_name)
                    data['keywords'] = keywords
                
                return Artist(**data)
        except:
            pass
        
        return Artist(
            artist=f"{artist_name} (아티스트명)" if "(" not in artist_name else artist_name,
            debut_date="",
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
