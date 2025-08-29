import requests
import time
import logging
from typing import Optional, Dict, List
import re

logger = logging.getLogger(__name__)

class MusixmatchLyricsAPI:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.musixmatch.com/ws/1.1"
        self.request_delay = 1  # API 요청 간 지연 시간 (초)
        
    def search_track(self, query: str = None, track_name: str = None, artist_name: str = None) -> Optional[Dict]:
        """
        트랙 검색
        Args:
            query: 검색 쿼리 (곡명 + 아티스트명)
            track_name: 곡명
            artist_name: 아티스트명
        Returns:
            검색된 곡 정보 또는 None
        """
        url = f"{self.base_url}/track.search"
        params = {
            "apikey": self.api_key,
            "page_size": 5,
            "page": 1,
            "s_track_rating": "desc"
        }
        
        if query:
            params["q"] = query
        elif track_name and artist_name:
            params["q_track"] = track_name
            params["q_artist"] = artist_name
        elif track_name:
            params["q_track"] = track_name
        else:
            logger.warning("검색 쿼리나 트랙명이 필요합니다")
            return None

        try:
            logger.info(f"Musixmatch API 검색: query='{query}', track='{track_name}', artist='{artist_name}'")
            response = requests.get(url, params=params, timeout=10)
            response.encoding = 'utf-8'  # 응답 인코딩을 UTF-8로 명시
            response.raise_for_status()

            data = response.json()
            
            if data.get("message", {}).get("header", {}).get("status_code") != 200:
                logger.warning(f"API 응답 오류: {data.get('message', {}).get('header', {}).get('hint', 'Unknown error')}")
                return None

            track_list = data.get("message", {}).get("body", {}).get("track_list", [])

            if not track_list:
                logger.warning(f"검색 결과 없음: query='{query}', track='{track_name}', artist='{artist_name}'")
                return None

            # 첫 번째 결과 반환
            first_track = track_list[0]["track"]
            track_info = {
                "track_id": first_track.get("track_id", ""),
                "track_name": first_track.get("track_name", ""),
                "artist_name": first_track.get("artist_name", ""),
                "album_name": first_track.get("album_name", ""),
                "has_lyrics": first_track.get("has_lyrics", 0),
                "has_subtitles": first_track.get("has_subtitles", 0)
            }
            
            logger.info(f"검색 성공: '{track_info['track_name']}' by {track_info['artist_name']}")
            return track_info

        except requests.RequestException as e:
            logger.error(f"Musixmatch API 검색 실패: {e}")
            return None
        except Exception as e:
            logger.error(f"검색 처리 중 오류: {e}")
            return None

    def get_lyrics(self, track_id: str) -> str:
        """
        트랙 ID로 가사 가져오기
        Args:
            track_id: Musixmatch 트랙 ID
        Returns:
            가사 텍스트 (가져오기 실패시 빈 문자열)
        """
        url = f"{self.base_url}/track.lyrics.get"
        params = {
            "apikey": self.api_key,
            "track_id": track_id
        }

        try:
            logger.info(f"가사 가져오기: track_id={track_id}")
            response = requests.get(url, params=params, timeout=15)
            response.encoding = 'utf-8'  # 응답 인코딩을 UTF-8로 명시
            response.raise_for_status()

            data = response.json()
            
            if data.get("message", {}).get("header", {}).get("status_code") != 200:
                logger.warning(f"가사 API 오류: {data.get('message', {}).get('header', {}).get('hint', 'Unknown error')}")
                return ""

            lyrics_data = data.get("message", {}).get("body", {}).get("lyrics", {})
            
            if not lyrics_data:
                logger.warning("가사 데이터가 없습니다")
                return ""

            lyrics_body = lyrics_data.get("lyrics_body", "")
            
            if not lyrics_body:
                logger.warning("가사 내용이 비어있습니다")
                return ""

            # 가사 정리
            cleaned_lyrics = self._clean_lyrics(lyrics_body)
            
            if cleaned_lyrics:
                return cleaned_lyrics
            else:
                logger.warning("가사 정리 후 내용이 비어있음")
                return ""

        except requests.RequestException as e:
            logger.error(f"가사 API 요청 실패: {e}")
            return ""
        except Exception as e:
            logger.error(f"가사 처리 중 오류: {e}")
            return ""

    def _clean_lyrics(self, raw_lyrics: str) -> str:
        """가사 텍스트 정리"""
        if not raw_lyrics:
            return ""
        
        # 문자열이 bytes인 경우 UTF-8로 디코드
        if isinstance(raw_lyrics, bytes):
            try:
                raw_lyrics = raw_lyrics.decode('utf-8')
            except UnicodeDecodeError:
                # UTF-8 실패시 다른 인코딩 시도
                try:
                    raw_lyrics = raw_lyrics.decode('utf-8', errors='ignore')
                except:
                    logger.error("가사 디코딩 실패")
                    return ""
        
        # Musixmatch 특유의 저작권 표시 제거
        cleaned = raw_lyrics.replace("******* This Lyrics is NOT for Commercial use *******", "")
        cleaned = re.sub(r'\*+.*?This.*?Commercial.*?\*+', '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'\(\d+\)', '', cleaned)  # (1) 같은 번호 제거
        
        # 기본 정리
        cleaned = cleaned.strip()
        
        # 여러 줄 바꿈을 두 줄 바꿈으로 정리
        cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
        
        # 앞뒤 공백 제거
        lines = [line.strip() for line in cleaned.split('\n')]
        cleaned = '\n'.join(line for line in lines if line)
        
        return cleaned

    def search_and_get_lyrics(self, song_title: str, artist_name: str) -> Dict[str, str]:
        """
        곡명과 아티스트로 검색 후 가사 가져오기
        Args:
            song_title: 곡명
            artist_name: 아티스트명
        Returns:
            가사 정보 딕셔너리
        """
        result = {
            "lyrics": "",
            "musixmatch_url": "",
            "status": "failed"
        }
        
        # 트랙 검색
        track_info = self.search_track(track_name=song_title, artist_name=artist_name)
        if not track_info:
            # 쿼리 방식으로 재시도
            query = f"{song_title} {artist_name}".strip()
            track_info = self.search_track(query=query)
            
        if not track_info:
            logger.warning(f"검색 실패: '{song_title}' by {artist_name}")
            return result
        
        # 가사가 있는지 확인
        if not track_info.get("has_lyrics", 0):
            logger.warning(f"가사가 없는 트랙: {track_info['track_name']}")
            result["status"] = "no_lyrics"
            return result
        
        # API 요청 간격 조절
        time.sleep(self.request_delay)
        
        # 가사 가져오기
        lyrics = self.get_lyrics(track_info["track_id"])
        
        result.update({
            "lyrics": lyrics,
            "musixmatch_url": f"https://www.musixmatch.com/lyrics/{track_info.get('artist_name', '').replace(' ', '-')}/{track_info.get('track_name', '').replace(' ', '-')}",
            "status": "success" if lyrics else "no_lyrics"
        })
        
        return result

    def bulk_fetch_lyrics(self, songs: List[Dict[str, str]], delay: float = 2.0) -> List[Dict[str, str]]:
        """
        여러 곡의 가사를 일괄 수집
        Args:
            songs: 곡 정보 리스트 [{"title": "곡명", "artist": "아티스트명"}, ...]
            delay: 요청 간 지연 시간
        Returns:
            가사가 포함된 곡 정보 리스트
        """
        results = []
        total = len(songs)
        
        logger.info(f"일괄 가사 수집 시작: {total}곡")
        
        for i, song in enumerate(songs, 1):
            logger.info(f"진행률: {i}/{total} - {song.get('title', 'Unknown')}")
            
            lyrics_info = self.search_and_get_lyrics(
                song.get("title", ""),
                song.get("artist", "")
            )
            
            # 원본 곡 정보에 가사 정보 추가
            updated_song = {**song}
            updated_song.update({
                "lyrics": lyrics_info["lyrics"],
                "musixmatch_url": lyrics_info["musixmatch_url"]
            })
            
            results.append(updated_song)
            
            # API 과부하 방지를 위한 지연
            if i < total:
                time.sleep(delay)
        
        success_count = sum(1 for r in results if r.get("lyrics"))
        logger.info(f"일괄 수집 완료: {success_count}/{total}곡 성공")
        
        return results

    def get_track_subtitle(self, track_id: str) -> str:
        """
        트랙 ID로 자막/번역 가져오기 (추가 기능)
        Args:
            track_id: Musixmatch 트랙 ID
        Returns:
            자막 텍스트 (가져오기 실패시 빈 문자열)
        """
        url = f"{self.base_url}/track.subtitle.get"
        params = {
            "apikey": self.api_key,
            "track_id": track_id
        }

        try:
            logger.info(f"자막 가져오기: track_id={track_id}")
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()

            data = response.json()
            
            if data.get("message", {}).get("header", {}).get("status_code") != 200:
                logger.warning(f"자막 API 오류: {data.get('message', {}).get('header', {}).get('hint', 'Unknown error')}")
                return ""

            subtitle_data = data.get("message", {}).get("body", {}).get("subtitle", {})
            
            if not subtitle_data:
                logger.warning("자막 데이터가 없습니다")
                return ""

            subtitle_body = subtitle_data.get("subtitle_body", "")
            return subtitle_body

        except requests.RequestException as e:
            logger.error(f"자막 API 요청 실패: {e}")
            return ""
        except Exception as e:
            logger.error(f"자막 처리 중 오류: {e}")
            return ""