#!/usr/bin/env python3
"""
데이터 파이프라인에서 Gemini API 사용 테스트
"""
import os
import sys
import logging
from pathlib import Path

# 프로젝트 루트 경로 추가
sys.path.insert(0, str(Path(__file__).parent))

from lib.config import Config
from core.apis.gemini_api import GeminiAPI
from lib.prompts import DataCollectionPrompts

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_pipeline_gemini():
    """파이프라인에서 사용하는 방식으로 Gemini API 테스트"""

    api_key = Config.GEMINI_API_KEY
    if not api_key:
        logger.error("GEMINI_API_KEY가 설정되지 않았습니다.")
        return

    try:
        logger.info("=== 파이프라인 Gemini API 테스트 시작 ===\n")
        gemini = GeminiAPI(api_key)

        # 실제 아티스트로 테스트
        test_artists = [
            {"artist_name": "Billie Eilish", "en_name": "Billie Eilish"},
            {"artist_name": "올리비아 로드리고", "en_name": "Olivia Rodrigo"},
            {"artist_name": "마룬 5", "en_name": "Maroon 5"}
        ]

        for artist_info in test_artists:
            logger.info(f"\n{'='*60}")
            logger.info(f"테스트 아티스트: {artist_info['artist_name']} ({artist_info['en_name']})")
            logger.info(f"{'='*60}")

            # 1. 아티스트 정보 수집
            artist_prompt = DataCollectionPrompts.get_artist_info_prompt(
                artist_info['artist_name']
            )

            logger.info("아티스트 정보 검색 중...")
            artist_response = gemini.query_json(
                artist_prompt,
                use_search=True
            )

            if artist_response:
                logger.info(f"✅ 아티스트 정보 응답 성공")
                logger.info(f"  데뷔: {artist_response.get('debut_date', 'N/A')}")
                logger.info(f"  카테고리: {artist_response.get('category', 'N/A')}")
                logger.info(f"  키워드: {artist_response.get('keywords', 'N/A')}")
                logger.info(f"  인스타그램: {artist_response.get('instagram', 'N/A')}")
            else:
                logger.warning("❌ 아티스트 정보 응답 실패")

            # 2. 예상 세트리스트 검색
            setlist_prompt = DataCollectionPrompts.get_expected_setlist_prompt(
                artist_info['artist_name'],
                "2024-2025 World Tour"  # 예제 콘서트 타이틀
            )

            logger.info("\n예상 세트리스트 검색 중...")
            setlist_response = gemini.query_json(
                setlist_prompt,
                use_search=True
            )

            if setlist_response:
                logger.info(f"✅ 세트리스트 응답 성공")
                if 'setlist' in setlist_response:
                    songs = setlist_response.get('setlist', [])
                    logger.info(f"발견된 곡: {len(songs)}개")
                    for i, song in enumerate(songs[:5], 1):  # 처음 5개만 출력
                        if isinstance(song, dict):
                            logger.info(f"  {i}. {song.get('order', '')}. {song.get('song_title', '')} ({song.get('duration', '')})")
                        else:
                            logger.info(f"  {i}. {song}")
                else:
                    logger.warning("응답에 'setlist' 필드가 없음")
            else:
                logger.warning("❌ 세트리스트 응답 실패")

        # 3. 모델 정보 출력
        logger.info(f"\n{'='*60}")
        logger.info("모델 정보")
        logger.info(f"{'='*60}")
        model_info = gemini.get_model_info()
        for key, value in model_info.items():
            logger.info(f"{key}: {value}")

        logger.info("\n✅ 파이프라인 테스트 완료!")

    except Exception as e:
        logger.error(f"테스트 중 오류 발생: {e}", exc_info=True)
        return False

    return True

if __name__ == "__main__":
    success = test_pipeline_gemini()
    sys.exit(0 if success else 1)