#!/usr/bin/env python3
"""
Musixmatch API를 사용한 가사 검색 모듈
"""
import requests
import logging
import time
from typing import Dict, Optional
import urllib.parse
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)

class MusixmatchLyricsAPI:
    """Musixmatch API 인터페이스"""
    
    BASE_URL = "https://api.musixmatch.com/ws/1.1"
    
    def __init__(self, api_key: str):
        """
        초기화
        Args:
            api_key: Musixmatch API 키
        """
        self.api_key = api_key
        
    def calculate_similarity(self, str1: str, str2: str) -> float:
        """
        두 문자열의 유사도 계산 (0.0 ~ 1.0)
        """
        # 소문자로 변환하고 공백 제거
        str1 = str1.lower().strip()
        str2 = str2.lower().strip()
        
        # 완전 일치
        if str1 == str2:
            return 1.0
            
        # 한쪽이 다른 쪽을 포함
        if str1 in str2 or str2 in str1:
            return 0.8
            
        # SequenceMatcher를 사용한 유사도 계산
        return SequenceMatcher(None, str1, str2).ratio()
        
    def search_track(self, title: str, artist: str) -> Optional[Dict]:
        """
        곡 검색
        Args:
            title: 곡 제목
            artist: 아티스트명 (원어)
        Returns:
            곡 정보 또는 None
        """
        try:
            # 검색 파라미터
            params = {
                'q_track': title,
                'q_artist': artist,
                'page_size': 1,
                'page': 1,
                's_track_rating': 'desc',
                'apikey': self.api_key
            }
            
            # API 요청
            response = requests.get(
                f"{self.BASE_URL}/track.search",
                params=params,
                timeout=10
            )
            
            if response.status_code != 200:
                logger.error(f"API 요청 실패: {response.status_code}")
                return None
            
            data = response.json()
            
            # 상태 코드 확인
            status_code = data.get('message', {}).get('header', {}).get('status_code')
            if status_code != 200:
                logger.warning(f"API 상태 코드: {status_code}")
                return None
            
            # 트랙 리스트 확인
            track_list = data.get('message', {}).get('body', {}).get('track_list', [])
            if not track_list:
                logger.info(f"검색 결과 없음: {title} - {artist}")
                return None
            
            # 첫 번째 결과 가져오기
            track = track_list[0].get('track', {})
            found_title = track.get('track_name', '')
            found_artist = track.get('artist_name', '')
            
            # 제목 유사도 검증
            title_similarity = self.calculate_similarity(title, found_title)
            
            # 아티스트 비교 시 feat. 부분 제거하고 메인 아티스트만 비교
            clean_original_artist = self._extract_main_artist(artist)
            clean_found_artist = self._extract_main_artist(found_artist)
            artist_similarity = self.calculate_similarity(clean_original_artist, clean_found_artist)
            
            logger.info(f"트랙 발견: {found_title} - {found_artist}")
            logger.info(f"유사도 - 제목: {title_similarity:.2f}, 아티스트: {artist_similarity:.2f}")
            
            # 제목과 아티스트 유사도 모두 확인 (AND 조건)
            # 제목 유사도가 너무 낮으면 거부 (임계값: 0.6)
            if title_similarity < 0.6:
                logger.warning(f"제목 유사도 너무 낮음! 원본: {title}, 발견: {found_title}, 유사도: {title_similarity:.2f}")
                return None
            
            # 아티스트 유사도가 너무 낮으면 거부 (임계값: 0.8)
            if artist_similarity < 0.8:
                logger.warning(f"아티스트 유사도 너무 낮음! 원본: {artist}, 발견: {found_artist}, 유사도: {artist_similarity:.2f}")
                return None
                
            return track
            
        except Exception as e:
            logger.error(f"트랙 검색 실패: {title} - {artist}: {e}")
            return None
    
    def get_lyrics_by_track_id(self, track_id: int) -> Optional[str]:
        """
        트랙 ID로 가사 가져오기
        Args:
            track_id: Musixmatch 트랙 ID
        Returns:
            가사 텍스트 또는 None
        """
        try:
            params = {
                'track_id': track_id,
                'apikey': self.api_key
            }
            
            response = requests.get(
                f"{self.BASE_URL}/track.lyrics.get",
                params=params,
                timeout=10
            )
            
            if response.status_code != 200:
                logger.error(f"가사 API 요청 실패: {response.status_code}")
                return None
            
            data = response.json()
            
            # 상태 코드 확인
            status_code = data.get('message', {}).get('header', {}).get('status_code')
            if status_code != 200:
                logger.warning(f"가사 API 상태 코드: {status_code}")
                return None
            
            # 가사 추출
            lyrics_obj = data.get('message', {}).get('body', {}).get('lyrics', {})
            lyrics_body = lyrics_obj.get('lyrics_body', '')
            
            if not lyrics_body:
                logger.warning(f"가사 본문 없음: track_id={track_id}")
                return None
            
            # Musixmatch 워터마크 제거
            if "******* This Lyrics is NOT for Commercial use *******" in lyrics_body:
                lyrics_body = lyrics_body.replace(
                    "******* This Lyrics is NOT for Commercial use *******", 
                    ""
                ).strip()
            
            # 줄바꿈 정리
            lyrics_body = lyrics_body.strip()
            
            return lyrics_body
            
        except Exception as e:
            logger.error(f"가사 가져오기 실패 track_id={track_id}: {e}")
            return None
    
    def get_lyrics(self, title: str, artist: str) -> Optional[Dict[str, str]]:
        """
        곡 제목과 아티스트로 가사 검색
        Args:
            title: 곡 제목
            artist: 아티스트명 (원어)
        Returns:
            {'lyrics': 가사, 'url': Musixmatch URL} 또는 None
        """
        try:
            # 1. 트랙 검색
            track = self.search_track(title, artist)
            if not track:
                # 괄호나 특수문자 제거 후 재시도
                clean_title = self._clean_search_query(title)
                clean_artist = self._clean_search_query(artist)
                if clean_title != title or clean_artist != artist:
                    logger.info(f"정제된 검색어로 재시도: {clean_title} - {clean_artist}")
                    track = self.search_track(clean_title, clean_artist)
                
                if not track:
                    logger.info(f"최종 검색 실패: {title} - {artist}")
                    return None
            
            track_id = track.get('track_id')
            if not track_id:
                logger.warning("트랙 ID를 찾을 수 없음")
                return None
            
            # 2. 가사 가져오기
            lyrics = self.get_lyrics_by_track_id(track_id)
            if not lyrics:
                return None
            
            # 3. Musixmatch URL 생성
            track_name = track.get('track_name', title)
            artist_name = track.get('artist_name', artist)
            commontrack_id = track.get('commontrack_id', '')
            
            # URL 생성 (실제 Musixmatch 페이지 형식)
            url_title = urllib.parse.quote(track_name.replace(' ', '-'))
            url_artist = urllib.parse.quote(artist_name.replace(' ', '-'))
            musixmatch_url = f"https://www.musixmatch.com/lyrics/{url_artist}/{url_title}"
            
            if commontrack_id:
                musixmatch_url = f"https://www.musixmatch.com/lyrics/{url_artist}/{url_title}/{commontrack_id}"
            
            return {
                'lyrics': lyrics,
                'url': musixmatch_url
            }
            
        except Exception as e:
            logger.error(f"가사 검색 실패: {title} - {artist}: {e}")
            return None
    
    def _extract_main_artist(self, artist_name: str) -> str:
        """
        아티스트 이름에서 메인 아티스트만 추출 (feat., ft., &, , 등 제거)
        """
        import re
        
        # feat., ft., featuring 이후 제거
        patterns = [
            r'\s+feat\..*$',
            r'\s+ft\..*$',
            r'\s+featuring.*$',
            r'\s+&.*$',
            r'\s*,.*$'  # 쉼표 이후도 제거
        ]
        
        cleaned = artist_name
        for pattern in patterns:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
        
        return cleaned.strip()
    
    def _clean_search_query(self, query: str) -> str:
        """
        검색어 정제 (괄호, 특수문자 제거)
        """
        # 괄호와 그 내용 제거
        import re
        cleaned = re.sub(r'\([^)]*\)', '', query)
        cleaned = re.sub(r'\[[^\]]*\]', '', cleaned)
        # feat. 이후 제거
        if 'feat.' in cleaned.lower():
            cleaned = cleaned[:cleaned.lower().index('feat.')]
        if 'ft.' in cleaned.lower():
            cleaned = cleaned[:cleaned.lower().index('ft.')]
        # 특수문자 제거 (일부 유지)
        cleaned = re.sub(r'[^\w\s\-\&\$]', '', cleaned)
        # 공백 정리
        cleaned = ' '.join(cleaned.split())
        return cleaned.strip()