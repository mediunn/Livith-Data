"""
데이터 수집 핵심 로직
"""
import logging
import time
import json
import re
from typing import Dict, Any, Optional
import requests
import pandas as pd
from lib.data_models import Concert, Artist
from lib.config import Config
from lib.prompts import DataCollectionPrompts
from core.apis.musicbrainz_api import MusicBrainzAPI
from core.apis.serper_api import SerperAPI

logger = logging.getLogger(__name__)


class DataCollector:
    def __init__(self, api_client):
        self.api = api_client
        self.mb_api = MusicBrainzAPI()
        self.serper = SerperAPI()

        self._artist_cache = {}
        self._title_artist_cache = {}

    def _validate_image_url(self, url: str) -> bool:
        """URL이 유효한 이미지인지 HEAD 요청으로 확인"""
        if not url:
            return False
        try:
            response = requests.head(url, timeout=5)
            response.raise_for_status()
            content_type = response.headers.get('Content-Type', '').lower()
            if 'image' in content_type:
                logger.info(f"이미지 URL 유효: {url}")
                return True
            else:
                logger.warning(f"이미지 URL 아님 (Content-Type: {content_type}): {url}")
                return False
        except requests.exceptions.RequestException as e:
            logger.warning(f"이미지 URL 검증 실패 {url}: {e}")
            return False
        except Exception as e:
            logger.error(f"이미지 URL 검증 중 오류 {url}: {e}")
            return False

    def collect_concert_basic_info(self, kopis_data: Dict[str, Any]) -> Concert:
        #KOPIS 데이터를 기반으로 기본 콘서트 정보 생성 (제목, 아티스트, 날짜, 장소, 포스터, 티켓URL, 상태)
        return Concert(
            title=kopis_data.get('title', ''),
            artist=kopis_data.get('artist', ''),
            start_date=self._format_date(kopis_data.get('start_date')),
            end_date=self._format_date(kopis_data.get('end_date')),
            venue=kopis_data.get('venue', ''),
            code=kopis_data.get('code', ''),
            poster=kopis_data.get('poster', ''),
            ticket_url=kopis_data.get('ticket_url', ''),  # KOPIS 티켓 URL 우선 사용
            status=self._determine_status(
                kopis_data.get('start_date'),
                kopis_data.get('end_date')
            )
        )

    def _extract_artist_from_title(self, title: str) -> Optional[str]:
        #AI를 사용해 콘서트 제목에서 아티스트 이름 추출 (아티스트명 한국어/영문)
        if title in self._title_artist_cache:
            logger.info(f"캐시에서 아티스트 반환: {title}")
            return self._title_artist_cache[title]
        try:
            query = DataCollectionPrompts.get_artist_name_prompt(title)
            response = self.api.query_json(query, use_search=True)
            time.sleep(6)

            if response and response.get('artist'):
                artist_name = response.get('artist')
                # 1글자 이하이거나 구분자만인 경우 거부
                if len(artist_name.strip()) <= 1 or artist_name.strip().lower() in ['x', '&', 'ft.', 'vs']:
                    logger.warning(f"유효하지 않은 아티스트명 거부: '{artist_name}'")
                    self._title_artist_cache[title] = None
                    return None
                logger.info(f"콘서트 제목 '{title}'에서 아티스트 '{artist_name}' 추출 성공")
                self._title_artist_cache[title] = artist_name
                return artist_name
        except Exception as e:
            logger.warning(f"콘서트 제목에서 아티스트 추출 실패 ({title}): {e}")
        self._title_artist_cache[title] = None
        return None

    def enhance_concert_data(self, concert: Concert) -> Concert:
        #AI API를 사용하여 콘서트 정보 보강 (아티스트명, 티켓URL, 한줄소개, 레이블)
        try:
            # 콘서트 제목에서 아티스트 추출하여 KOPIS 출연진 정보 덮어쓰기
            try:
                extracted_artist = self._extract_artist_from_title(concert.title)
                if extracted_artist and extracted_artist != concert.artist:
                    logger.info(f"콘서트 제목에서 추출한 아티스트 '{extracted_artist}'로 기존 정보 '{concert.artist}'를 덮어씁니다.")
                    concert.artist = extracted_artist
            except Exception as e:
                logger.warning(f"콘서트 제목에서 아티스트 추출 중 오류 발생: {e}")

            # 티켓 정보 수집 (KOPIS URL 없을 때만 Serper 검색)
            if not concert.ticket_url:
                ticket_info = self.serper.search_ticket_url(concert.title)
                if ticket_info:
                    concert.ticket_url = ticket_info.get('url', '')
                    concert.ticket_site = ticket_info.get('site', '')

            concert.introduction = self._collect_short_introduction(concert.title, concert.artist)

            additional_info = self._collect_additional_info(concert.title, concert.artist)
            if additional_info:
                concert.label = additional_info.get('label', '')

            return concert

        except Exception as e:
            logger.error(f"콘서트 정보 보강 실패: {e}")
            return concert

    def collect_artist_info(self, artist_name: str, concert_title: Optional[str] = None) -> Optional[Artist]:
        #아티스트 정보 수집 (카테고리, 소개, 인스타URL, 키워드, 이미지, 데뷔년도, 국적, 그룹유형, MBID)
        try:
            info = self._collect_artist_basic_info(artist_name, concert_title)

            if not info:
                return None

            return Artist(
                artist=info.get('artist', artist_name),
                category=info.get('category', ''),
                detail=info.get('detail', ''),
                instagram_url=info.get('instagram_url', ''),
                keywords=info.get('keywords', ''),
                img_url=info.get('img_url', ''),
                debut_date=info.get('debut_date', ''),
                nationality=info.get('nationality', ''),
                group_type=info.get('group_type', ''),
                musicbrainz_id=info.get('musicbrainz_id', '')
            )

        except Exception as e:
            logger.error(f"아티스트 정보 수집 실패: {e}")
            return None

    def collect_concert_genre(self, artist_name: str, concert_title: str) -> Optional[Dict[str, Any]]:
        #콘서트 장르 정보 수집 (장르명, 장르 코드)
        try:
            query = DataCollectionPrompts.get_concert_genre_prompt(artist_name, concert_title)
            response = self.api.query_json(query, use_search=True)
            time.sleep(6)

            if response:
                if isinstance(response, dict):
                    return response
                elif isinstance(response, list) and len(response) > 0:
                    return response[0]

        except Exception as e:
            logger.warning(f"콘서트 장르 수집 실패 ({concert_title}): {e}")

        return None

    def _format_date(self, date_str: Optional[str]) -> str:
        #날짜 형식 통일 (YYYY.MM.DD)
        if not date_str:
            return ''
        return date_str.replace('-', '.').replace('/', '.')

    def _determine_status(self, start_date: Optional[str], end_date: Optional[str]) -> str:
        """날짜 기반 상태 결정"""
        from datetime import datetime, date

        if not start_date:
            return 'UNKNOWN'

        try:
            start_date = start_date.replace('-', '.').replace('/', '.')
            start = datetime.strptime(start_date, '%Y.%m.%d').date()
            today = date.today()

            if start > today:
                return 'UPCOMING'
            elif end_date:
                end_date = end_date.replace('-', '.').replace('/', '.')
                end = datetime.strptime(end_date, '%Y.%m.%d').date()
                return 'ONGOING' if end >= today else 'PAST'
            else:
                return 'PAST' if start < today else 'ONGOING'

        except ValueError:
            return 'UNKNOWN'

    def _collect_additional_info(self, title: str, artist: str) -> Optional[Dict[str, Any]]:
        #추가 정보 수집 (레이블)
        try:
            query = DataCollectionPrompts.get_additional_info_prompt(title, artist)
            response = self.api.query_json(query, use_search=True)
            time.sleep(6)

            if response:
                return {'label': response.get('label', '')}

        except Exception as e:
            logger.warning(f"추가 정보 수집 실패: {e}")

        return None

    def _collect_short_introduction(self, title: str, artist: str) -> str:
        #한 줄 요약 소개 수집 (콘서트 한줄소개)
        try:
            query = DataCollectionPrompts.get_short_introduction_prompt(title, artist)
            response = self.api.query_json(query, use_search=True)
            time.sleep(6)

            if response:
                introduction = response.get('summary') or response.get('introduction', '')

                if introduction:
                    introduction = re.sub(r"'\s*'", "", introduction)
                    introduction = re.sub(r",\s*의 주인공", "의 주인공", introduction)
                    introduction = re.sub(r"'\s*,\s*의", "의", introduction)
                    introduction = re.sub(r"히트곡\s*의\s+주인공\s*", "", introduction)
                    introduction = re.sub(r"^의\s+주인공\s*", "", introduction)
                    introduction = re.sub(r"주요곡\s*,\s*'", "주요곡 '", introduction)
                    introduction = re.sub(r"\s+", " ", introduction).strip()

                return introduction

        except Exception as e:
            logger.warning(f"한 줄 요약 수집 실패: {e}")

        return ""

    def _collect_artist_basic_info(self, artist_name: str, concert_title: Optional[str] = None) -> Optional[Dict[str, Any]]:
        #아티스트 기본 정보 수집 (MusicBrainz 우선, LLM 보강) (국적, 그룹유형, 데뷔년도, 카테고리, 소개, 이미지, 인스타URL, 키워드)
        if artist_name in self._artist_cache:
            logger.info(f"캐시에서 아티스트 정보 반환: {artist_name}")
            return self._artist_cache[artist_name]

        artist_info = {}
        musicbrainz_id = ""

        try:
            # 대표곡 목록 수집
            song_examples = []
            try:
                song_query = DataCollectionPrompts.get_artist_songs_prompt(artist_name)
                song_response = self.api.query_json(song_query, use_search=True)
                time.sleep(6)

                if song_response and song_response.get('songs'):
                    song_examples = [s for s in song_response.get('songs', []) if s and s.strip()][:2]
                    if song_examples:
                        logger.info(f"'{artist_name}' 대표곡 수집 성공: {song_examples}")
                    else:
                        logger.warning(f"'{artist_name}' 대표곡 응답은 있지만 비어있음")
                else:
                    logger.warning(f"'{artist_name}' 대표곡 검색 결과 없음")
            except Exception as e:
                logger.warning(f"대표곡 검색 실패: {e}")

            # 1. MusicBrainz에서 아티스트 정보 검색
            mb_artists = self.mb_api.search_artist(artist_name, limit=3)

            if mb_artists:
                best_match = None
                for mb_artist in mb_artists:
                    if int(mb_artist.get('score', 0)) >= 90 or mb_artist.get('name').lower() == artist_name.lower():
                        best_match = mb_artist
                        break
                if not best_match:
                    best_match = mb_artists[0]

                if best_match:
                    musicbrainz_id = best_match['id']
                    logger.info(f"MusicBrainz에서 '{artist_name}' 정보 발견 (MBID: {musicbrainz_id})")

                    artist_info['artist'] = artist_name
                    artist_info['musicbrainz_id'] = musicbrainz_id

                    mb_type = best_match.get('type')
                    if mb_type == 'Person':
                        artist_info['group_type'] = '솔로'
                    elif mb_type == 'Group':
                        artist_info['group_type'] = '그룹'
                    else:
                        artist_info['group_type'] = ''

                    artist_info['nationality'] = best_match.get('country', '')

                    mb_details = self.mb_api.get_artist_by_id(musicbrainz_id)
                    artist_details = mb_details.get('artist', {})
                    # 그룹만 MusicBrainz 결성일 사용 (솔로는 생년이 나오므로 LLM에 맡김)
                    if mb_type == 'Group' and artist_details and 'life-span' in artist_details and artist_details['life-span'].get('begin'):
                        artist_info['debut_date'] = artist_details['life-span']['begin'].split('-')[0]
                    else:
                        artist_info['debut_date'] = ''
            else:
                logger.info(f"MusicBrainz에서 '{artist_name}' 정보 없음. LLM으로 대체.")

            # 2. LLM을 사용하여 정보 보강
            mb_context = f"MusicBrainz 정보: {json.dumps(artist_info, ensure_ascii=False)}" if artist_info else ""
            logger.debug(f"MusicBrainz 컨텍스트: {mb_context}")

            query = DataCollectionPrompts.get_artist_basic_info_prompt(
                artist_name,
                concert_title,
                musicbrainz_context=mb_context,
                song_examples=song_examples
            )
            response = self.api.query_json(query, use_search=True)
            time.sleep(6)

            if response:
                artist_info['category'] = response.get('category', '')
                artist_info['detail'] = response.get('detail', '')

                # 빈 따옴표/공백 따옴표가 포함된 문장 전체 제거
                detail = artist_info.get('detail', '')
                if detail:
                    detail = re.sub(r"[^.]*'\s*'[^.]*\.", '', detail)
                    detail = re.sub(r'\s+', ' ', detail).strip()
                    artist_info['detail'] = detail

                llm_img_url = response.get('img_url', '')
                if llm_img_url and self._validate_image_url(llm_img_url):
                    artist_info['img_url'] = llm_img_url
                else:
                    artist_info['img_url'] = ''

                if not artist_info.get('instagram_url'):
                    artist_info['instagram_url'] = response.get('instagram_url', '')

                artist_info['keywords'] = response.get('keywords', '')

                if not artist_info.get('artist'):
                    artist_info['artist'] = response.get('artist', artist_name)
                if not artist_info.get('debut_date'):
                    artist_info['debut_date'] = response.get('debut_date', '')
                if not artist_info.get('nationality'):
                    artist_info['nationality'] = response.get('nationality', '')
                if not artist_info.get('group_type'):
                    artist_info['group_type'] = response.get('group_type', '')

                if musicbrainz_id:
                    artist_info['musicbrainz_id'] = musicbrainz_id

                self._artist_cache[artist_name] = artist_info
                return artist_info
            elif artist_info:
                self._artist_cache[artist_name] = artist_info
                return artist_info

        except Exception as e:
            logger.warning(f"아티스트 정보 수집 실패: {e}", exc_info=True)

        return None
