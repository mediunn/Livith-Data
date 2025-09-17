import json
import time
import logging
import re
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
from core.apis.perplexity_api import PerplexityAPI
from lib.data_models import *
from core.apis.artist_name_mapper import ArtistNameMapper
from lib.config import Config
from lib.prompts import DataCollectionPrompts

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
        date_str = f"{kopis_concert.get('start_date', '')}" 
        ticket_info = self._collect_ticket_info(concert_title, final_artist_name, date_str)
        
        # 날짜 기반 상태 결정
        status = self._determine_status_from_dates(
            self._format_date(kopis_concert['start_date']),
            self._format_date(kopis_concert['end_date'])
        )
        
        # label과 introduction 정보 수집
        label_intro_info = self._collect_label_introduction(final_artist_name, concert_title)
        
        # 기본 콘서트 정보 생성 - KOPIS 데이터를 데이터 모델에 맞게 매핑
        concert = Concert(
            artist=artist_display,  # 표기용 아티스트명 (기존 artist_display)
            code=kopis_concert.get('code', ''),  # KOPIS 공연 코드
            title=concert_title,
            start_date=self._format_date(kopis_concert['start_date']),
            end_date=self._format_date(kopis_concert['end_date']),
            status=status,
            poster=kopis_concert.get('poster', ''),
            ticket_site=ticket_info.get('site', ''),
            ticket_url=ticket_info.get('url', ''),
            venue=self._clean_venue_name(kopis_concert.get('venue', '')),
            label=label_intro_info.get('label', ''),
            introduction=label_intro_info.get('introduction', '')
        )
        
        # 상세 데이터 수집 - CSV 파일별로 하나씩 순차적으로 수집
        logger.info("=" * 50)
        logger.info(f"상세 데이터 수집 시작: {concert_title}")
        logger.info("=" * 50)
        
        # 1. 셋리스트 정보 수집
        logger.info("[1/8] 셋리스트 정보 수집 중...")
        setlists = self._collect_setlists(concert_title, final_artist_name, status)
        for setlist in setlists:
            setlist.start_date = concert.start_date
            setlist.end_date = concert.end_date
            setlist.venue = concert.venue
            setlist.img_url = concert.poster  # 콘서트 포스터를 셋리스트 이미지로 사용
        time.sleep(Config.REQUEST_DELAY)
        
        # 2. 곡 정보 수집
        logger.info("[2/8] 곡 정보 수집 중...")
        setlist_songs, songs = self._collect_songs_data(setlists, final_artist_name)
        time.sleep(Config.REQUEST_DELAY)
        
        # 3. 콘서트-셋리스트 연결 정보 생성
        logger.info("[3/8] 콘서트-셋리스트 연결 정보 생성 중...")
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
        
        # 4. 문화 정보 수집
        logger.info("[4/8] 문화 정보 수집 중...")
        cultures = self._collect_cultures(concert_title, final_artist_name, concert)
        time.sleep(Config.REQUEST_DELAY)
        
        # 5. 스케줄 정보 수집
        logger.info("[5/8] 스케줄 정보 수집 중...")
        schedules = self._collect_schedules(concert_title, final_artist_name, concert.start_date, concert.end_date)
        time.sleep(Config.REQUEST_DELAY)
        
        # 6. 콘서트 상세 정보 수집
        logger.info("[6/7] 콘서트 상세 정보 수집 중...")
        concert_info = self._collect_concert_info(concert_title, final_artist_name)
        time.sleep(Config.REQUEST_DELAY)
        
        # 7. 아티스트 정보 수집
        logger.info("[7/7] 아티스트 정보 수집 중...")
        artist_info = self._collect_artist_info(final_artist_name)
        time.sleep(Config.REQUEST_DELAY)
        
        # 9. 장르 정보 수집
        logger.info("[보너스] 장르 정보 수집 중...")
        concert_genres = self._collect_concert_genres(concert_title, final_artist_name)
        
        logger.info("=" * 50)
        logger.info(f"상세 데이터 수집 완료: {concert_title}")
        logger.info("=" * 50)
        
        return {
            'concert': concert,
            'setlists': setlists,
            'concert_setlists': concert_setlists,
            'setlist_songs': setlist_songs,
            'songs': songs,
            'cultures': cultures,
            'schedules': schedules,
            'concert_info': concert_info,
            'artist': artist_info,
            'concert_genres': concert_genres
        }
    
    @staticmethod
    def sort_concerts(concerts: List[Concert]) -> List[Concert]:
        """콘서트 목록을 상태와 날짜 기준으로 정렬"""
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
                img_url="",     # 나중에 콘서트 포스터로 채움
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
                img_url="",     # 나중에 콘서트 포스터로 채움
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
                img_url="",     # 나중에 콘서트 포스터로 채움
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
        logger.info(f"_collect_songs_data 시작: setlists 수={len(setlists)}, artist={artist_name}")
        all_setlist_songs = []
        all_songs = []
        valid_setlists = []  # 유효한 셋리스트만 저장
        
        for i, setlist in enumerate(setlists):
            logger.info(f"셋리스트 처리 중 [{i+1}/{len(setlists)}]: {setlist.title}")
            # 예상 셋리스트인지 과거 셋리스트인지 확인
            if "예상 셋리스트" in setlist.title:
                # 예상 셋리스트 수집 - 무조건 15곡 이상 생성
                prompt = DataCollectionPrompts.get_expected_setlist_prompt(artist_name, setlist.title)
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
                
                prompt = DataCollectionPrompts.get_actual_setlist_prompt(artist_name, setlist.title, setlist.venue, setlist.start_date)
            
            # 셋리스트 수집
            logger.info(f"셋리스트 수집 중: {setlist.title}")
            
            # 예상 셋리스트는 최대 3번 재시도
            max_retries = 3 if "예상 셋리스트" in setlist.title else 1
            setlist_songs, songs = [], []
            
            for attempt in range(max_retries):
                logger.info(f"API 호출 중 (시도 {attempt + 1}/{max_retries})...")
                response = self.api.query_with_search(prompt, context="셋리스트 수집")
                logger.info(f"API 응답 받음, 파싱 시작...")
                try:
                    setlist_songs, songs = self._parse_and_validate_songs(response, setlist, artist_name)
                    logger.info(f"파싱 완료: setlist_songs={len(setlist_songs)}, songs={len(songs)}")
                except Exception as e:
                    import traceback
                    logger.error(f"파싱 중 오류 발생: {e}")
                    logger.error(f"스택 트레이스: {traceback.format_exc()}")
                    setlist_songs, songs = [], []
                
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
            
            # 셋리스트 유형에 따른 처리 - 모든 셋리스트 10곡 이상 기준
            if "예상 셋리스트" in setlist.title:
                # 예상 셋리스트는 10곡 이상일 때만 포함
                if len(songs) >= 10:
                    all_setlist_songs.extend(setlist_songs)
                    all_songs.extend(songs)
                    valid_setlists.append(setlist)
                    if len(songs) >= 15:
                        logger.info(f"✅ 예상 셋리스트 {len(songs)}곡 수집 완료 (목표 달성)")
                    else:
                        logger.warning(f"⚠️ 예상 셋리스트 {len(songs)}곡 수집 완료 (최소 기준 충족)")
                else:
                    logger.error(f"❌ 예상 셋리스트 곡이 {len(songs)}개로 10곡 미만, 제외")
            elif "실제 셋리스트" in setlist.title:
                # 실제 셋리스트도 10곡 이상일 때만 추가
                if len(songs) >= 10:
                    all_setlist_songs.extend(setlist_songs)
                    all_songs.extend(songs)
                    valid_setlists.append(setlist)
                    logger.info(f"✅ 실제 셋리스트 {len(songs)}곡 수집 완료")
                else:
                    logger.warning(f"실제 셋리스트 곡이 10개 미만 ({len(songs)}개), 제외")
            else:
                # 기타 (호환성용) - 10곡 이상 기준
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
    
    def _collect_cultures(self, concert_title: str, artist_name: str, concert) -> List[Culture]:
        """문화 정보 수집"""
        prompt = DataCollectionPrompts.get_culture_info_prompt(artist_name, concert_title, concert)
        
        # JSON 형식 강제를 위해 query_json 사용
        json_prompt = f"{prompt}\n\n중요: 반드시 유효한 JSON 배열 형식으로만 응답하세요. 설명이나 추가 텍스트는 포함하지 마세요."
        response = self.api.query_json(json_prompt)
        return self._parse_cultures(response, concert_title)
    
    def _collect_schedules(self, concert_title: str, artist_name: str, start_date: str, end_date: str) -> List[Schedule]:
        """스케줄 정보 수집"""
        prompt = DataCollectionPrompts.get_schedule_info_prompt(artist_name, concert_title, start_date, end_date)
        
        response = self.api.query_with_search(prompt)
        schedules = self._parse_schedules(response, concert_title)
        
        # 스케줄을 찾지 못한 경우 기본 콘서트 스케줄 추가
        if not schedules:
            schedules.append(Schedule(
                concert_title=concert_title,
                category="콘서트",
                scheduled_at=start_date
            ))
            logger.info(f"기본 콘서트 스케줄 생성: {start_date}")
        
        return schedules
    
    def _collect_merchandise(self, concert_title: str, artist_name: str) -> List[Merchandise]:
        """MD 상품 정보 수집"""
        prompt = DataCollectionPrompts.get_merchandise_prompt(artist_name, concert_title)
        
        # JSON 형식 강제를 위해 query_json 사용
        json_prompt = f"{prompt}\n\n중요: 반드시 유효한 JSON 배열 형식으로만 응답하세요."
        response = self.api.query_json(json_prompt)
        return self._parse_merchandise(response, concert_title)
    
    def _collect_concert_genres(self, concert_title: str, artist_name: str) -> List[ConcertGenre]:
        """콘서트 장르 정보 수집"""
        prompt = DataCollectionPrompts.get_concert_genre_prompt(artist_name, concert_title)
        
        response = self.api.query_with_search(prompt, context="장르 분류")
        return self._parse_concert_genres(response, concert_title)
    
    def _collect_concert_info(self, concert_title: str, artist_name: str) -> List[ConcertInfo]:
        """콘서트 정보 수집"""
        logger.info(f"콘서트 정보 수집 시작: {artist_name} - {concert_title}")
        prompt = DataCollectionPrompts.get_concert_info_prompt(artist_name, concert_title)
        
        # JSON 형식 강제를 위해 query_json 사용
        json_prompt = f"{prompt}\n\n중요: 반드시 유효한 JSON 배열 형식으로만 응답하세요."
        logger.debug(f"콘서트 정보 프롬프트: {json_prompt[:500]}...")
        
        response = self.api.query_json(json_prompt)
        logger.info(f"콘서트 정보 API 응답 받음: {type(response)}, 길이: {len(response) if isinstance(response, (list, dict)) else len(str(response))}")
        logger.debug(f"콘서트 정보 응답 내용: {response}")
        
        result = self._parse_concert_info(response, concert_title)
        logger.info(f"콘서트 정보 파싱 결과: {len(result)}개 항목")
        return result
    
    def _collect_artist_info(self, artist_name: str) -> Optional[Artist]:
        """아티스트 정보 수집"""
        prompt = DataCollectionPrompts.get_artist_info_prompt(artist_name)
        
        response = self.api.query_with_search(prompt)
        return self._parse_artist_info(response, artist_name)
    
    def _collect_label_introduction(self, artist_name: str, concert_title: str) -> Dict[str, str]:
        """콘서트 라벨과 소개 정보 수집"""
        prompt = DataCollectionPrompts.get_concert_label_introduction_prompt(artist_name, concert_title)
        
        try:
            response = self.api.query_with_search(prompt)
            json_str = self._extract_json_from_response(response, '{', '}')
            if json_str:
                import json
                data = json.loads(json_str)
                
                # introduction 검증 및 정리
                intro = data.get('introduction', '').strip()
                
                # 부적절한 내용 필터링
                invalid_phrases = ['정보가 없다', '검색할 수 없다', '찾을 수 없다', '알 수 없다', '정보가 부족하다', '확인할 수 없다']
                if any(phrase in intro for phrase in invalid_phrases) or len(intro) < 10:
                    intro = f"{artist_name}의 특별한 라이브 무대! 대표곡들과 함께하는 {concert_title}"
                
                return {
                    'label': data.get('label', '').strip(),
                    'introduction': intro
                }
        except Exception as e:
            logger.error(f"Label/Introduction 수집 실패 ({artist_name} - {concert_title}): {e}")
        
        # 기본값 반환 - introduction은 항상 의미있는 내용으로 채우기
        return {
            'label': '',
            'introduction': f"{artist_name}의 특별한 라이브 무대! 대표곡들과 함께하는 {concert_title}"
        }
    
    def collect_merchandise_data(self, concert: Concert) -> List[Dict[str, str]]:
        """콘서트의 굿즈(merchandise) 정보를 수집합니다."""
        
        prompt = DataCollectionPrompts.get_merchandise_prompt(concert.artist, concert.title)
        
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
        """AI 검색으로만 아티스트 정보 검색 - fallback 로직 제거"""
        # AI 검색으로 아티스트 정보 검색 (최대 3번 재시도)
        for attempt in range(3):
            searched_artist = self._search_artist_from_concert(concert_title)
            
            if searched_artist and len(searched_artist) > 1:
                logger.info(f"AI 검색으로 아티스트 발견 (시도 {attempt + 1}/3): '{concert_title}' -> '{searched_artist}'")
                return searched_artist
            
            logger.warning(f"AI 아티스트 검색 실패, 재시도 {attempt + 1}/3")
        
        # 모든 시도 실패 시 KOPIS 원본 데이터 사용
        if original_artist and original_artist.strip():
            logger.warning(f"AI 검색 완전 실패, KOPIS 원본 데이터 사용: '{original_artist}'")
            return original_artist.strip()
        
        # 최후의 수단: 콘서트 제목 정리해서 사용
        logger.error(f"모든 아티스트 정보 수집 실패, 콘서트 제목 사용: '{concert_title}'")
        return concert_title.replace("[서울]", "").replace("내한공연", "").strip()

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
        
        # KOPIS 아티스트 정보가 너무 길거나 이상한 경우 (멤버 이름 나열 등) 검증
        if kopis_artist and kopis_artist.strip():
            kopis_clean = kopis_artist.strip()
            
            # 이상한 패턴 감지 (쉼표로 구분된 긴 이름들, 너무 긴 텍스트 등)
            if (',' in kopis_clean and len(kopis_clean) > 50) or len(kopis_clean) > 100:
                logger.warning(f"KOPIS 아티스트명이 이상함, API로 검색: {kopis_clean[:50]}...")
            elif len(kopis_clean) > 1 and len(kopis_clean) < 50:
                # 정상적인 길이의 KOPIS 데이터는 그대로 사용
                logger.info(f"KOPIS 아티스트 정보 사용: {kopis_clean}")
                return kopis_clean
        
        # KOPIS 정보가 없거나 이상한 경우 퍼플렉시티로 검색
        prompt = DataCollectionPrompts.get_artist_display_prompt(concert_title, artist_name, kopis_artist if kopis_artist else "")
        
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

    def _collect_ticket_info(self, concert_title: str, artist_name: str, date: str = "") -> Dict[str, str]:
        """퍼플렉시티 API로 티켓 예매 정보 수집 (사이트명과 URL)"""
        # 추가적인 날짜 정보가 있으면 프롬프트에 추가
        if date:
            prompt = f"{DataCollectionPrompts.get_ticket_info_prompt(artist_name, concert_title)}\n일시: {date}\n\nJSON 형식으로 응답: {{\"ticket_site\": \"예매사이트명\", \"ticket_url\": \"URL또는빈문자열\"}}"
        else:
            prompt = f"{DataCollectionPrompts.get_ticket_info_prompt(artist_name, concert_title)}\n\nJSON 형식으로 응답: {{\"ticket_site\": \"예매사이트명\", \"ticket_url\": \"URL또는빈문자열\"}}"
        
        try:
            response = self.api.query_with_search(prompt)
            json_str = self._extract_json_from_response(response, '{', '}')
            if json_str:
                data = json.loads(json_str)
                site = data.get('ticket_site', '').strip()
                url = data.get('ticket_url', '').strip()
                
                # 유효한 정보가 있는 경우 반환 (사이트명이나 URL 중 하나라도 있으면 OK)
                if site or (url and url.startswith('http')):
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

    def _clean_venue_name(self, venue: str) -> str:
        """장소명에서 괄호 안의 내용 제거"""
        if not venue:
            return ""
        
        import re
        # 괄호와 그 안의 내용 제거 (소괄호, 대괄호 모두)
        cleaned = re.sub(r'\([^)]*\)', '', venue)  # (내용) 제거
        cleaned = re.sub(r'\[[^\]]*\]', '', cleaned)  # [내용] 제거
        
        # 연속된 공백 정리
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        return cleaned
    
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
        """곡 데이터를 파싱하고 songs와 setlist_songs 데이터를 동기화하여 반환"""
        try:
            logger.info(f"_parse_and_validate_songs 시작: setlist={setlist.title}, artist={artist_name}")
            json_str = self._extract_json_from_response(response, '{', '}')
            if json_str:
                data = json.loads(json_str)
                
                # setlist_songs 데이터를 먼저 파싱
                valid_setlist_songs = []
                setlist_song_titles = set()  # 중복 제거용
                
                for item in data.get('setlist_songs', []):
                    if isinstance(item, dict) and item.get('song_title', '').strip():
                        current_song_title = item.get('song_title', '').strip()
                        if current_song_title not in setlist_song_titles:
                            # 모든 셋리스트에 대해 콘서트 날짜로 setlist_date 수정
                            if hasattr(setlist, 'start_date') and setlist.start_date:
                                item['setlist_date'] = setlist.start_date
                            valid_setlist_songs.append(SetlistSong(**item))
                            setlist_song_titles.add(current_song_title)
                
                # songs 데이터를 setlist_songs와 동기화하여 생성
                valid_songs = []
                song_titles = set()  # 중복 제거용
                
                # 1. setlist_songs에 있는 곡들을 기반으로 songs 생성
                for setlist_song in valid_setlist_songs:
                    target_song_title = setlist_song.song_title
                    if target_song_title not in song_titles:
                        # songs 배열에서 해당 곡 찾기
                        song_data = None
                        for item in data.get('songs', []):
                            if isinstance(item, dict) and item.get('title', '').strip() == target_song_title:
                                song_data = item
                                break
                        
                        # 찾지 못한 경우 기본 Song 객체 생성
                        if not song_data:
                            song_data = {
                                'title': target_song_title,
                                'artist': artist_name,
                                'lyrics': '',
                                'pronunciation': '',
                                'translation': '',
                                'youtube_id': ''
                            }
                        
                        valid_songs.append(Song(**song_data))
                        song_titles.add(target_song_title)
                
                # 2. songs 배열에만 있고 setlist_songs에 없는 곡들도 추가 (기존 데이터 호환성)
                for item in data.get('songs', []):
                    if isinstance(item, dict) and item.get('title', '').strip():
                        additional_song_title = item.get('title', '').strip()
                        if additional_song_title not in song_titles:
                            valid_songs.append(Song(**item))
                            song_titles.add(additional_song_title)
                
                logger.info(f"동기화된 곡 데이터: setlist_songs={len(valid_setlist_songs)}, songs={len(valid_songs)}")
                return valid_setlist_songs, valid_songs
        except Exception as e:
            import traceback
            logger.error(f"곡 데이터 파싱 실패: {e}")
            logger.error(f"스택 트레이스: {traceback.format_exc()}")
        
        return [], []
    
    def _parse_songs_data(self, response: str, setlist: Setlist, artist_name: str) -> Tuple[List[SetlistSong], List[Song]]:
        """기존 함수 - 호환성을 위해 유지"""
        return self._parse_and_validate_songs(response, setlist, artist_name)
    
    def _parse_cultures(self, response, concert_title: str) -> List[Culture]:
        try:
            # response가 이미 list인 경우 (query_json 응답)
            if isinstance(response, list):
                data = response
            # response가 문자열인 경우 JSON 추출
            elif isinstance(response, str):
                # API 응답에서 불필요한 텍스트 제거
                cleaned_response = response
                # "Google Search를 통해" 같은 설명 텍스트 제거
                if "Google Search" in cleaned_response or "검색하여" in cleaned_response:
                    # JSON 배열 부분만 추출
                    import re
                    json_match = re.search(r'\[\s*\{[\s\S]*?\}\s*\]', cleaned_response)
                    if json_match:
                        json_str = json_match.group()
                    else:
                        json_str = self._extract_json_from_response(cleaned_response, '[', ']')
                else:
                    json_str = self._extract_json_from_response(cleaned_response, '[', ']')
                
                if json_str:
                    data = json.loads(json_str)
                else:
                    logger.warning("JSON 추출 실패, 빈 리스트 반환")
                    return []
            else:
                return []
            
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
                    
                    # Google Search 관련 텍스트 제거
                    content = self._remove_search_artifacts(content)
                    
                    # 출처 표시 제거
                    content = self._remove_sources(content)
                    
                    # 말투 통일 (해요체)
                    content = self._normalize_tone(content)
                    
                    img_url = item.get('img_url', '').strip()
                    culture_data = {
                        'concert_title': item.get('concert_title', concert_title),
                        'title': title,
                        'content': content,
                        'img_url': img_url
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
                    content=content + "..." if len(response) > 500 else content,
                    img_url=""
                )]
        
        # 완전히 실패한 경우 해당 아티스트나 장르의 추정 문화 정보 제공
        if "indie" in artist_name.lower() or "웹스터" in artist_name or "indie" in concert_title.lower():
            return [
                Culture(
                    concert_title=concert_title,
                    title="인디 콘서트 특유의 친밀한 분위기",
                    content="인디 아티스트들의 콘서트는 대형 공연장보다는 소규모 라이브하우스에서 열리는 경우가 많아, 아티스트와 관객 간의 거리가 가깝습니다. 공연 중 아티스트가 직접 관객과 대화하는 시간이 많고, 편안하고 자유로운 분위기에서 진행됩니다.",
                    img_url="https://images.unsplash.com/photo-1493225457124-a3eb161ffa5f?w=800"
                ),
                Culture(
                    concert_title=concert_title,
                    title="조용한 감상 문화",
                    content="인디/얼터너티브 장르 특성상 서정적인 곡들이 많아, 팬들은 조용히 음악에 집중하며 감상하는 문화가 발달되어 있습니다. 큰 소리로 떼창하기보다는 가사에 집중하고, 아티스트의 감정을 함께 느끼는 것을 중요하게 생각합니다.",
                    img_url="https://images.unsplash.com/photo-1540039155733-5bb30b53aa14?w=800"
                )
            ]
        elif "jazz" in artist_name.lower() or "jazz" in concert_title.lower() or "알 디 메올라" in artist_name:
            return [
                Culture(
                    concert_title=concert_title,
                    title="재즈 공연의 즉흥연주 감상법",
                    content="재즈 콘서트에서는 즉흥연주(improvisation)가 중요한 부분을 차지합니다. 관객들은 연주자의 기교적인 솔로 연주 후 박수를 치는 것이 관례이며, 특히 뛰어난 연주에는 '브라보'나 휘파람으로 감탄을 표현하기도 합니다.",
                    img_url="https://images.unsplash.com/photo-1493225457124-a3eb161ffa5f?w=800"
                ),
                Culture(
                    concert_title=concert_title,
                    title="앉아서 감상하는 문화",
                    content="재즈 공연은 음악의 섬세함과 복잡함을 집중해서 들어야 하기 때문에, 대부분 앉아서 조용히 감상하는 것이 일반적입니다. 휴대폰 사용을 자제하고, 연주 중에는 대화를 피하는 것이 매너입니다.",
                    img_url="https://images.unsplash.com/photo-1459749411175-04bf5292ceea?w=800"
                )
            ]
        else:
            return [
                Culture(
                    concert_title=concert_title,
                    title="이 공연만의 특별한 순간",
                    content="모든 라이브 공연에는 그 순간에만 경험할 수 있는 특별함이 있습니다. 아티스트와 관객이 함께 만들어가는 유일무이한 경험을 통해 음악의 진정한 매력을 느낄 수 있습니다.",
                    img_url="https://images.unsplash.com/photo-1501386761578-eac5c94b800a?w=800"
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
    
    def _parse_merchandise(self, response, concert_title: str) -> List[Merchandise]:
        try:
            # response가 이미 list인 경우 (query_json 응답)
            if isinstance(response, list):
                data = response
            # response가 문자열인 경우 JSON 추출
            elif isinstance(response, str):
                json_str = self._extract_json_from_response(response, '[', ']')
                if json_str:
                    data = json.loads(json_str)
                else:
                    return []
            else:
                return []
                
            merchandise_list = []
            for item in data:
                if isinstance(item, dict):
                    # 필드명 매핑 (item_name -> name)
                    if 'item_name' in item:
                        item['name'] = item.pop('item_name')
                    
                    # artist_name 필드가 있으면 제거 (Merchandise 모델에 없음)
                    if 'artist_name' in item:
                        item.pop('artist_name')
                        
                    # 필요하지 않은 필드들 제거
                    for unnecessary_field in ['availability', 'description']:
                        if unnecessary_field in item:
                            item.pop(unnecessary_field)
                    
                    # 가격 형식을 nn,nnn원 형태로 변환
                    if 'price' in item:
                        price = str(item['price'])
                        # 숫자만 추출하고 천 단위 구분자 추가
                        import re
                        numbers = re.findall(r'\d+', price.replace(',', ''))
                        if numbers:
                            num = int(numbers[0])
                            formatted_price = f"{num:,}원"
                            item['price'] = formatted_price
                    merchandise_list.append(Merchandise(**item))
            return merchandise_list
        except Exception as e:
            logger.error(f"굿즈 파싱 실패: {e}")
            return []
    
    def _parse_concert_genres(self, response: str, concert_title: str) -> List[ConcertGenre]:
        try:
            json_str = self._extract_json_from_response(response, '[', ']')
            if json_str:
                data = json.loads(json_str)
                genres = []
                for item in data:
                    if isinstance(item, dict):
                        # 필수 필드 검증
                        if all(key in item for key in ['concert_id', 'concert_title', 'genre_id', 'name']):
                            genres.append(ConcertGenre(**item))
                
                if genres:
                    return genres
        except Exception as e:
            logger.error(f"콘서트 장르 파싱 실패: {e}")
        
        # 파싱 실패 시 AI에게 다시 간단히 물어보기
        logger.warning(f"콘서트 장르 파싱 실패, 간단한 장르 분류 재시도: {concert_title}")
        return self._get_fallback_genre(concert_title)
    
    def _get_fallback_genre(self, concert_title: str) -> List[ConcertGenre]:
        """장르 파싱 실패 시 간단한 프롬프트로 재시도"""
        try:
            fallback_prompt = DataCollectionPrompts.get_concert_genre_fallback_prompt(concert_title)

            response = self.api.query_with_search(fallback_prompt)
            json_str = self._extract_json_from_response(response, '{', '}')
            
            if json_str:
                data = json.loads(json_str)
                genre_id = data.get('genre_id', 1)
                name = data.get('name', 'JPOP')
                
                return [ConcertGenre(
                    concert_id=concert_title,
                    concert_title=concert_title,
                    genre_id=genre_id,
                    name=name
                )]
        except Exception as e:
            logger.error(f"Fallback 장르 분류도 실패: {e}")
        
        # 최후의 수단: JPOP 기본값
        logger.error(f"모든 장르 분류 실패, JPOP으로 강제 할당: {concert_title}")
        return [ConcertGenre(
            concert_id=concert_title,
            concert_title=concert_title,
            genre_id=1,
            name="JPOP"
        )]
    
    def _parse_concert_info(self, response, concert_title: str) -> List[ConcertInfo]:
        logger.info(f"concert_info 파싱 시작: {concert_title}")
        try:
            # response가 이미 list인 경우 (query_json 응답)
            if isinstance(response, list):
                data = response
                logger.debug(f"응답이 list 형태, 항목 수: {len(data)}")
            # response가 문자열인 경우 JSON 추출
            elif isinstance(response, str):
                json_str = self._extract_json_from_response(response, '[', ']')
                if json_str:
                    data = json.loads(json_str)
                    logger.debug(f"JSON 추출 성공, 항목 수: {len(data)}")
                else:
                    logger.warning("JSON 추출 실패 - 빈 배열 반환")
                    return []
            else:
                logger.warning(f"예상치 못한 응답 타입: {type(response)}")
                return []
                
            concert_infos = []
            for i, item in enumerate(data):
                if isinstance(item, dict):
                    # content가 비어있거나 너무 짧은 경우 해당 항목 제외
                    content = item.get('content', '')
                    category = item.get('category', 'Unknown')
                    
                    logger.debug(f"항목 {i+1} 검사: category='{category}', content 길이={len(str(content))}")
                    
                    # 빈 content나 무의미한 내용 필터링
                    if not content or not str(content).strip():
                        logger.debug(f"concert_info content가 비어있어 제외: category='{category}'")
                        continue
                        
                    content = str(content).strip()
                    
                    # 너무 짧은 내용만 제외 (기준 완화: 10자 → 5자)
                    if len(content) < 5:
                        logger.debug(f"concert_info content가 너무 짧아 제외: category='{category}', content='{content[:20]}...'")
                        continue
                        
                    # 무의미한 응답 필터링 (기준 완화)
                    meaningless_phrases = [
                        "정보를 찾을 수 없습니다",
                        "찾을 수 없습니다"
                    ]
                    
                    # 완전히 무의미한 내용만 제외 (부분 매칭에서 전체 매칭으로 완화)
                    is_meaningless = any(content.strip() == phrase for phrase in meaningless_phrases)
                    if is_meaningless:
                        logger.debug(f"concert_info 무의미한 content로 제외: category='{category}'")
                        continue
                        
                    # Google Search 관련 텍스트 제거
                    content = self._remove_search_artifacts(content)
                    # content를 해요체로 변하
                    content = self._normalize_tone(content)
                    item['content'] = content
                    logger.debug(f"concert_info 항목 추가: category='{category}'")
                    concert_infos.append(ConcertInfo(**item))
            
            logger.info(f"concert_info 파싱 완료: {len(concert_infos)}개 항목 추가됨")
            return concert_infos
            
        except Exception as e:
            logger.error(f"concert_info 파싱 중 오류: {e}")
            logger.debug(f"오류 발생 시 응답: {response}")
        
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
            artist=artist_name,
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
    
    def _remove_search_artifacts(self, text: str) -> str:
        """Google Search 관련 텍스트 및 마크다운 제거"""
        import re
        if not text:
            return text
        
        # Google Search 관련 문구 제거
        search_patterns = [
            r'Google Search를 통해[^.]*\.',
            r'검색하여 정리했[^.]*\.',
            r'검색 결과[^.]*\.',
            r'정보를 찾[^.]*\.',
            r'\*\*[^*]+\*\*',  # 마크다운 볼드 제거
            r'\*[^*]+\*',  # 마크다운 이탤릭 제거
            r'^---+$',  # 구분선 제거
        ]
        
        cleaned_text = text
        for pattern in search_patterns:
            cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.MULTILINE | re.IGNORECASE)
        
        # 연속된 공백 및 줄바꿈 정리
        cleaned_text = re.sub(r'\n\s*\n', '\n', cleaned_text)
        cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
        
        return cleaned_text
    
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
        
        # ~니어요로 끝나는 이상한 어미를 ~다로 수정
        # 예: 했답니어요 → 했답니다, 그렇답니어요 → 그렇답니다
        normalized_text = re.sub(r'([가-힣]+)니어요([\.!?]?)', r'\1니다\2', normalized_text)
        
        # 추가로 ~어니어요 패턴도 처리
        normalized_text = re.sub(r'([가-힣]+)어니어요([\.!?]?)', r'\1었습니다\2', normalized_text)
        
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
