"""
MusicBrainz API 클라이언트
아티스트 기본 정보(국적, 그룹 유형, 인스타url, 트위터url,데뷔일 등) 수집에 사용
"""
import musicbrainzngs
import logging
from lib.config import Config

logger = logging.getLogger(__name__)


class MusicBrainzAPI:
    def __init__(self):
        musicbrainzngs.set_useragent(
            "LivithDataProject",
            "0.1",
            Config.CONTACT_EMAIL
        )

    def search_artist(self, artist_name: str, limit: int = 5) -> list:
        #아티스트 이름으로 검색, 결과 목록 반환
        try:
            result = musicbrainzngs.search_artists(query=artist_name, limit=limit)
            return [
                {
                    'id': a.get('id'),
                    'name': a.get('name'),
                    'sort-name': a.get('sort-name'),
                    'disambiguation': a.get('disambiguation'),
                    'country': a.get('country'),
                    'type': a.get('type'),
                    'score': a.get('ext:score')
                }
                for a in result['artist-list']
            ]
        except musicbrainzngs.WebServiceError as e:
            logger.error(f"MusicBrainz 검색 실패 '{artist_name}': {e}")
            return []
        except Exception as e:
            logger.error(f"MusicBrainz 검색 중 오류 '{artist_name}': {e}")
            return []

    def get_artist_by_id(self, mbid: str) -> dict:
        #MBID로 아티스트 상세 정보 조회
        try:
            return musicbrainzngs.get_artist_by_id(mbid, includes=['url-rels', 'tags'])
        except musicbrainzngs.WebServiceError as e:
            logger.error(f"MusicBrainz 조회 실패 '{mbid}': {e}")
            return {}
        except Exception as e:
            logger.error(f"MusicBrainz 조회 중 오류 '{mbid}': {e}")
            return {}
