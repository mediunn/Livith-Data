import json
import time
import logging
import re
from typing import List, Optional, Dict, Any, Union, Tuple
from src.perplexity_api import PerplexityAPI
from src.data_models import Concert, Setlist, SetlistSong, Song, Culture
from config import Config

logger = logging.getLogger(__name__)

class ConcertDataCollector:
    def __init__(self, perplexity_api: PerplexityAPI):
        self.api = perplexity_api
    
    def get_artist_concerts(self, artist_name: str) -> List[Concert]:
        """콘서트 정보 수집"""
        prompt = f"""Search for verified concert information about {artist_name}.

Find actual concerts and provide response as valid JSON array only. Example format:
[{{"title": "Concert Name", "type": "WORLD TOUR", "status": "COMPLETED", "date": "2024-12-01", "img_url": "", "artist": "{artist_name}"}}]

Important: Return only valid JSON with no additional text or explanations."""
        
        response = self.api.query_with_search(prompt)
        return self._parse_concerts_response(response, artist_name)
    
    def get_concert_setlists(self, concert_title: str, artist_name: str) -> List[Setlist]:
        """특정 콘서트의 셋리스트 정보 수집"""
        prompt = f"""Find setlist information for {artist_name}'s "{concert_title}" concert.

Provide response as valid JSON array only. Example format:
[{{"concert_title": "{concert_title}", "title": "Main Setlist", "type": "PAST", "status": "종료"}}]

Important: Return only valid JSON with no additional text."""
        
        response = self.api.query_with_search(prompt)
        return self._parse_setlists_response(response, concert_title)
    
    def get_setlist_songs(self, setlist_title: str, artist_name: str) -> Tuple[List[SetlistSong], List[Song]]:
        """특정 셋리스트의 곡 목록과 곡 정보 수집"""
        prompt = f"""Find songs for "{setlist_title}" by {artist_name}.

Provide response as valid JSON object only. Example format:
{{"setlist_songs": [{{"setlist_title": "{setlist_title}", "song_title": "Song Name", "song_artist": "{artist_name}", "order_index": 1, "fanchant": ""}}], "songs": [{{"title": "Song Name", "artist": "{artist_name}", "img_url": "", "lyrics": "", "pronunciation": "", "translation": "", "album": "", "release_date": "", "genre": ""}}]}}

Important: Return only valid JSON with no additional text."""
        
        response = self.api.query_with_search(prompt)
        return self._parse_setlist_songs_response(response, setlist_title, artist_name)
    
    def get_concert_culture(self, concert_title: str, artist_name: str) -> Optional[Culture]:
        """콘서트의 문화적 맥락 수집"""
        prompt = f"""Find cultural significance of {artist_name}'s "{concert_title}" concert.

Provide response as valid JSON object only. Example format:
{{"concert_title": "{concert_title}", "content": "Cultural information about this concert"}}

Important: Return only valid JSON with no additional text."""
        
        response = self.api.query_with_search(prompt)
        return self._parse_culture_response(response, concert_title)
    
    def _clean_json_response(self, response: str) -> str:
        """응답에서 JSON을 정리하고 검증"""
        if not response:
            return ""
        
        # 일반적인 문제 문자들 수정
        cleaned = response.strip()
        
        # 잘못된 따옴표 수정
        cleaned = re.sub(r"'([^']*)':", r'"\1":', cleaned)  # 키의 단일 따옴표를 이중 따옴표로
        cleaned = re.sub(r":\s*'([^']*)'", r': "\1"', cleaned)  # 값의 단일 따옴표를 이중 따옴표로
        
        # 후행 쉼표 제거
        cleaned = re.sub(r',\s*}', '}', cleaned)
        cleaned = re.sub(r',\s*]', ']', cleaned)
        
        # 제어 문자 제거
        cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)
        
        # 이스케이프되지 않은 백슬래시 처리
        cleaned = re.sub(r'\\(?!["\\/bfnrt])', r'\\\\', cleaned)
        
        return cleaned
    
    def _extract_json_from_response(self, response: str, start_char: str, end_char: str) -> str:
        """응답에서 JSON 추출 및 정리"""
        try:
            cleaned_response = self._clean_json_response(response)
            
            start_idx = cleaned_response.find(start_char)
            if start_idx == -1:
                logger.warning(f"JSON 시작 문자 '{start_char}'를 찾을 수 없음")
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
            
            # JSON 유효성 검증
            try:
                json.loads(json_str)
                return json_str
            except json.JSONDecodeError as e:
                logger.error(f"추출된 JSON이 유효하지 않음: {e}")
                logger.debug(f"문제가 있는 JSON: {json_str[:200]}...")
                return ""
            
        except Exception as e:
            logger.error(f"JSON 추출 중 오류: {e}")
            return ""
    
    def _filter_valid_fields(self, cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """클래스에 정의된 필드만 필터링"""
        if hasattr(cls, '__dataclass_fields__'):
            valid_fields = set(cls.__dataclass_fields__.keys())
            filtered_data = {}
            
            for key, value in data.items():
                if key in valid_fields:
                    filtered_data[key] = value
                else:
                    logger.debug(f"예상치 못한 필드 제외: {key} = {value}")
            
            return filtered_data
        return data
    
    def _safe_create_object(self, cls, data: Union[Dict[str, Any], Any]) -> Optional[Any]:
        """안전한 객체 생성"""
        try:
            if not isinstance(data, dict):
                logger.error(f"예상한 딕셔너리가 아닌 타입: {type(data)}")
                return None
            
            # 유효한 필드만 필터링
            filtered_data = self._filter_valid_fields(cls, data)
            
            # 클래스별 필수 필드 및 기본값 정의
            field_defaults = {
                Concert: {
                    'title': 'Unknown Concert',
                    'type': 'Concert',
                    'status': 'Unknown',
                    'date': '2025-01-01',
                    'img_url': '',
                    'artist': 'Unknown Artist'
                },
                Setlist: {
                    'concert_title': 'Unknown Concert',
                    'title': 'Unknown Setlist',
                    'type': 'PAST',
                    'status': '종료'
                },
                SetlistSong: {
                    'setlist_title': 'Unknown Setlist',
                    'song_title': 'Unknown Song',
                    'song_artist': 'Unknown Artist',
                    'order_index': 1,
                    'fanchant': ''
                },
                Song: {
                    'title': 'Unknown Song',
                    'artist': 'Unknown Artist',
                    'img_url': '',
                    'lyrics': '',
                    'pronunciation': '',
                    'translation': ''
                },
                Culture: {
                    'concert_title': 'Unknown Concert',
                    'content': 'Cultural information not available'
                }
            }
            
            defaults = field_defaults.get(cls, {})
            
            # 필수 필드 보장
            for field, default_value in defaults.items():
                if field not in filtered_data or filtered_data[field] is None:
                    filtered_data[field] = default_value
                    logger.debug(f"필수 필드 '{field}' 기본값 설정")
            
            return cls(**filtered_data)
                
        except Exception as e:
            logger.error(f"객체 생성 실패: {e}")
            logger.debug(f"클래스: {cls.__name__}")
            logger.debug(f"필터링된 데이터: {filtered_data if 'filtered_data' in locals() else 'N/A'}")
            return None
    
    def _parse_concerts_response(self, response: str, artist_name: str) -> List[Concert]:
        try:
            json_str = self._extract_json_from_response(response, '[', ']')
            if json_str and json_str != "[]":
                data = json.loads(json_str)
                
                if not isinstance(data, list):
                    logger.error(f"콘서트 응답이 배열이 아님: {type(data)}")
                    return []
                
                results = []
                for item in data:
                    if isinstance(item, dict):
                        obj = self._safe_create_object(Concert, item)
                        if obj:
                            results.append(obj)
                
                logger.info(f"{len(results)}개의 콘서트 정보 파싱 완료")
                return results
            else:
                logger.warning(f"{artist_name}의 콘서트 정보를 찾을 수 없습니다")
                
                # 기본 콘서트 정보 생성
                default_concert = Concert(
                    title=f"{artist_name} Concert Tour",
                    type="Tour",
                    status="COMPLETED",
                    date="2024-01-01",
                    img_url="",
                    artist=artist_name
                )
                return [default_concert]
                
        except (json.JSONDecodeError, TypeError) as e:
            logger.error(f"콘서트 응답 파싱 실패: {e}")
            logger.debug(f"원본 응답: {response[:500]}...")
            
            # 파싱 실패 시 기본 콘서트 정보 생성
            default_concert = Concert(
                title=f"{artist_name} Concert Tour",
                type="Tour",
                status="COMPLETED",
                date="2024-01-01",
                img_url="",
                artist=artist_name
            )
            return [default_concert]
    
    def _parse_setlists_response(self, response: str, concert_title: str) -> List[Setlist]:
        try:
            json_str = self._extract_json_from_response(response, '[', ']')
            if json_str and json_str != "[]":
                data = json.loads(json_str)
                
                if not isinstance(data, list):
                    logger.error(f"셋리스트 응답이 배열이 아님: {type(data)}")
                    return []
                
                results = []
                for item in data:
                    if isinstance(item, dict):
                        item['concert_title'] = concert_title
                        obj = self._safe_create_object(Setlist, item)
                        if obj:
                            results.append(obj)
                
                return results
            else:
                logger.warning(f"{concert_title}의 셋리스트 정보를 찾을 수 없습니다")
                
                # 기본 셋리스트 생성
                default_setlist = Setlist(
                    concert_title=concert_title,
                    title=f"{concert_title} Main Setlist",
                    type="PAST",
                    status="종료"
                )
                return [default_setlist]
                
        except (json.JSONDecodeError, TypeError) as e:
            logger.error(f"셋리스트 응답 파싱 실패: {e}")
            logger.debug(f"원본 응답: {response[:500]}...")
            
            # 파싱 실패 시 기본 셋리스트 생성
            default_setlist = Setlist(
                concert_title=concert_title,
                title=f"{concert_title} Main Setlist",
                type="PAST",
                status="종료"
            )
            return [default_setlist]
    
    def _parse_setlist_songs_response(self, response: str, setlist_title: str, artist_name: str) -> Tuple[List[SetlistSong], List[Song]]:
        try:
            json_str = self._extract_json_from_response(response, '{', '}')
            if json_str:
                data = json.loads(json_str)
                
                if not isinstance(data, dict):
                    logger.error(f"셋리스트 곡 응답이 딕셔너리가 아님: {type(data)}")
                    return [], []
                
                setlist_songs = []
                songs = []
                
                if 'setlist_songs' in data and isinstance(data['setlist_songs'], list):
                    for item in data['setlist_songs']:
                        if isinstance(item, dict):
                            item['setlist_title'] = setlist_title
                            item['song_artist'] = artist_name
                            obj = self._safe_create_object(SetlistSong, item)
                            if obj:
                                setlist_songs.append(obj)
                
                if 'songs' in data and isinstance(data['songs'], list):
                    for item in data['songs']:
                        if isinstance(item, dict):
                            item['artist'] = artist_name
                            obj = self._safe_create_object(Song, item)
                            if obj:
                                songs.append(obj)
                
                return setlist_songs, songs
            else:
                logger.warning(f"{setlist_title}의 곡 정보를 찾을 수 없습니다")
                return [], []
                
        except (json.JSONDecodeError, TypeError) as e:
            logger.error(f"셋리스트 곡 응답 파싱 실패: {e}")
            logger.debug(f"원본 응답: {response[:500]}...")
        return [], []
    
    def _parse_culture_response(self, response: str, concert_title: str) -> Optional[Culture]:
        try:
            json_str = self._extract_json_from_response(response, '{', '}')
            if json_str:
                data = json.loads(json_str)
                
                if isinstance(data, dict):
                    data['concert_title'] = concert_title
                    return self._safe_create_object(Culture, data)
                else:
                    logger.error(f"문화 정보 응답이 딕셔너리가 아님: {type(data)}")
            else:
                logger.warning(f"{concert_title}의 문화 정보를 찾을 수 없습니다")
                
        except (json.JSONDecodeError, TypeError) as e:
            logger.error(f"문화 정보 응답 파싱 실패: {e}")
            logger.debug(f"원본 응답: {response[:500]}...")
        
        # 기본 문화 정보 생성
        return Culture(
            concert_title=concert_title,
            content="문화적 정보를 수집할 수 없습니다"
        )
