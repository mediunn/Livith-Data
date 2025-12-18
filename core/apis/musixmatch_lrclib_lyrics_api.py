#!/usr/bin/env python3
"""
Musixmatch + LRCLIB 폴백 가사 검색 모듈
"""
import requests
import logging
import re
from typing import Dict, Optional
import urllib.parse
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)


class MusixmatchAPI:
    """Musixmatch API 인터페이스"""
    
    BASE_URL = "https://api.musixmatch.com/ws/1.1"
    
    def __init__(self, api_key: str):
        self.api_key = api_key
    
    def _calculate_similarity(self, str1: str, str2: str) -> float:
        str1 = str1.lower().strip()
        str2 = str2.lower().strip()
        
        if str1 == str2:
            return 1.0
        if str1 in str2 or str2 in str1:
            return 0.8
        return SequenceMatcher(None, str1, str2).ratio()
    
    def _extract_main_artist(self, artist_name: str) -> str:
        patterns = [
            r'\s+feat\..*$',
            r'\s+ft\..*$',
            r'\s+featuring.*$',
            r'\s+&.*$',
            r'\s*,.*$'
        ]
        cleaned = artist_name
        for pattern in patterns:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
        return cleaned.strip()
    
    def _clean_search_query(self, query: str) -> str:
        cleaned = re.sub(r'\([^)]*\)', '', query)
        cleaned = re.sub(r'\[[^\]]*\]', '', cleaned)
        if 'feat.' in cleaned.lower():
            cleaned = cleaned[:cleaned.lower().index('feat.')]
        if 'ft.' in cleaned.lower():
            cleaned = cleaned[:cleaned.lower().index('ft.')]
        cleaned = re.sub(r'[^\w\s\-\&\$]', '', cleaned)
        cleaned = ' '.join(cleaned.split())
        return cleaned.strip()
    
    def search_track(self, title: str, artist: str) -> Optional[Dict]:
        try:
            params = {
                'q_track': title,
                'q_artist': artist,
                'page_size': 5,
                'page': 1,
                's_track_rating': 'desc',
                'apikey': self.api_key
            }
            
            response = requests.get(
                f"{self.BASE_URL}/track.search",
                params=params,
                timeout=10
            )
            
            if response.status_code != 200:
                logger.error(f"Musixmatch API 요청 실패: {response.status_code}")
                return None
            
            data = response.json()
            status_code = data.get('message', {}).get('header', {}).get('status_code')
            if status_code != 200:
                logger.warning(f"Musixmatch API 상태 코드: {status_code}")
                return None
            
            track_list = data.get('message', {}).get('body', {}).get('track_list', [])
            if not track_list:
                logger.info(f"Musixmatch 검색 결과 없음: {title} - {artist}")
                return None
            
            for item in track_list:
                track = item.get('track', {})
                found_title = track.get('track_name', '')
                found_artist = track.get('artist_name', '')
                
                title_similarity = self._calculate_similarity(title, found_title)
                clean_original_artist = self._extract_main_artist(artist)
                clean_found_artist = self._extract_main_artist(found_artist)
                artist_similarity = self._calculate_similarity(clean_original_artist, clean_found_artist)
                
                logger.info(f"Musixmatch 후보: {found_title} - {found_artist} (유사도: 제목 {title_similarity:.2f}, 아티스트 {artist_similarity:.2f})")
                
                if title_similarity < 0.8:
                    continue
                if artist_similarity < 0.9:
                    continue
                
                logger.info(f"✅ Musixmatch 트랙 선택: {found_title} - {found_artist}")
                return track
            
            logger.warning(f"Musixmatch 모든 후보 트랙 유사도 기준 미달: {title} - {artist}")
            return None
            
        except Exception as e:
            logger.error(f"Musixmatch 트랙 검색 실패: {title} - {artist}: {e}")
            return None
    
    def get_lyrics_by_track_id(self, track_id: int) -> Optional[str]:
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
                logger.error(f"Musixmatch 가사 API 요청 실패: {response.status_code}")
                return None
            
            data = response.json()
            status_code = data.get('message', {}).get('header', {}).get('status_code')
            if status_code != 200:
                logger.warning(f"Musixmatch 가사 API 상태 코드: {status_code}")
                return None
            
            lyrics_obj = data.get('message', {}).get('body', {}).get('lyrics', {})
            lyrics_body = lyrics_obj.get('lyrics_body', '')
            
            if not lyrics_body:
                logger.warning(f"Musixmatch 가사 본문 없음: track_id={track_id}")
                return None
            
            if "******* This Lyrics is NOT for Commercial use *******" in lyrics_body:
                lyrics_body = lyrics_body.replace(
                    "******* This Lyrics is NOT for Commercial use *******", 
                    ""
                ).strip()
            
            return lyrics_body.strip()
            
        except Exception as e:
            logger.error(f"Musixmatch 가사 가져오기 실패 track_id={track_id}: {e}")
            return None
    
    def get_lyrics(self, title: str, artist: str) -> Optional[Dict[str, str]]:
        try:
            track = self.search_track(title, artist)
            if not track:
                clean_title = self._clean_search_query(title)
                clean_artist = self._clean_search_query(artist)
                if clean_title != title or clean_artist != artist:
                    logger.info(f"Musixmatch 정제된 검색어로 재시도: {clean_title} - {clean_artist}")
                    track = self.search_track(clean_title, clean_artist)
                
                if not track:
                    return None
            
            track_id = track.get('track_id')
            if not track_id:
                return None
            
            lyrics = self.get_lyrics_by_track_id(track_id)
            if not lyrics:
                return None
            
            track_name = track.get('track_name', title)
            artist_name = track.get('artist_name', artist)
            url_title = urllib.parse.quote(track_name.replace(' ', '-'))
            url_artist = urllib.parse.quote(artist_name.replace(' ', '-'))
            musixmatch_url = f"https://www.musixmatch.com/lyrics/{url_artist}/{url_title}"
            
            return {
                'lyrics': lyrics,
                'url': musixmatch_url,
                'source': 'musixmatch'
            }
            
        except Exception as e:
            logger.error(f"Musixmatch 가사 검색 실패: {title} - {artist}: {e}")
            return None


class LrcLibAPI:
    """LRCLIB API 인터페이스 (폴백용)"""
    
    BASE_URL = "https://lrclib.net/api"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Livith/1.0 (https://livith.kr)'
        })
    
    def _calculate_similarity(self, str1: str, str2: str) -> float:
        str1 = str1.lower().strip()
        str2 = str2.lower().strip()
        
        if str1 == str2:
            return 1.0
        if str1 in str2 or str2 in str1:
            return 0.8
        return SequenceMatcher(None, str1, str2).ratio()
    
    def get_lyrics(self, title: str, artist: str) -> Optional[Dict[str, str]]:
        try:
            response = self.session.get(
                f"{self.BASE_URL}/search",
                params={
                    'track_name': title,
                    'artist_name': artist
                },
                timeout=10
            )
            
            if response.status_code != 200:
                logger.warning(f"LRCLIB 검색 실패: {response.status_code}")
                return None
            
            results = response.json()
            if not results:
                logger.info(f"LRCLIB 검색 결과 없음: {title} - {artist}")
                return None
            
            # 유사도 검증하며 결과 순회
            for track in results:
                found_title = track.get('trackName', '')
                found_artist = track.get('artistName', '')
                
                title_sim = self._calculate_similarity(title, found_title)
                artist_sim = self._calculate_similarity(artist, found_artist)
                
                logger.info(f"LRCLIB 후보: {found_title} - {found_artist} (유사도: 제목 {title_sim:.2f}, 아티스트 {artist_sim:.2f})")
                
                # Musixmatch보다 느슨하게 (0.7)
                if title_sim < 0.7:
                    continue
                if artist_sim < 0.7:
                    continue
                
                plain_lyrics = track.get('plainLyrics')
                if not plain_lyrics:
                    continue
                
                logger.info(f"✅ LRCLIB 가사 찾음: {found_title} - {found_artist}")
                return {
                    'lyrics': plain_lyrics,
                    'source': 'lrclib'
                }
            
            logger.warning(f"LRCLIB 모든 후보 유사도 기준 미달: {title} - {artist}")
            return None
            
        except Exception as e:
            logger.error(f"LRCLIB 검색 오류: {e}")
            return None


class LyricsAPI:
    """통합 가사 API (Musixmatch → LRCLIB 폴백)"""
    
    def __init__(self, musixmatch_api_key: str):
        self.musixmatch = MusixmatchAPI(musixmatch_api_key)
        self.lrclib = LrcLibAPI()
    
    def get_lyrics(self, title: str, artist: str) -> Optional[Dict[str, str]]:
        """
        가사 검색 (Musixmatch 우선, 실패 시 LRCLIB 폴백)
        
        Returns:
            {'lyrics': 가사, 'source': 'musixmatch'|'lrclib', 'url': ...} 또는 None
        """
        # 1. Musixmatch 시도
        result = self.musixmatch.get_lyrics(title, artist)
        if result:
            return result
        
        # 2. LRCLIB 폴백
        logger.info(f"Musixmatch 실패, LRCLIB 폴백: {title} - {artist}")
        return self.lrclib.get_lyrics(title, artist)