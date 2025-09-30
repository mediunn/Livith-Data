#!/usr/bin/env python3
"""
Gemini API 테스트 스크립트
"""
import os
import sys
import logging
from pathlib import Path

# 프로젝트 루트 경로 추가
sys.path.insert(0, str(Path(__file__).parent))

from lib.config import Config
from core.apis.gemini_api import GeminiAPI

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_gemini_api():
    """Gemini API 테스트"""

    # API 키 확인
    api_key = Config.GEMINI_API_KEY
    if not api_key:
        logger.error("GEMINI_API_KEY가 설정되지 않았습니다.")
        return

    logger.info(f"API 키 발견: {api_key[:10]}...")

    try:
        # Gemini API 클라이언트 초기화
        logger.info("Gemini API 클라이언트 초기화 중...")
        gemini = GeminiAPI(api_key)

        # 1. 간단한 쿼리 테스트
        logger.info("\n=== 1. 일반 쿼리 테스트 ===")
        prompt = "안녕하세요. 간단히 답변해주세요."
        response = gemini.query(prompt, use_search=False)
        logger.info(f"응답: {response[:200]}")

        # 2. Google Search를 사용한 쿼리 테스트
        logger.info("\n=== 2. Google Search 쿼리 테스트 ===")
        search_prompt = "2025년 1월 한국 내한 콘서트 정보를 간단히 알려주세요."
        search_response = gemini.query_with_search(search_prompt, search_focus=True)
        logger.info(f"검색 응답: {search_response[:500]}")

        # 3. JSON 응답 테스트
        logger.info("\n=== 3. JSON 응답 테스트 ===")
        json_prompt = """
        다음 형식의 JSON을 반환해주세요:
        {
            "test": "success",
            "model": "gemini-2.5-flash",
            "timestamp": "현재시각"
        }
        """
        json_response = gemini.query_json(json_prompt, use_search=False)
        logger.info(f"JSON 응답: {json_response}")

        # 4. 모델 정보 확인
        logger.info("\n=== 4. 모델 정보 ===")
        model_info = gemini.get_model_info()
        for key, value in model_info.items():
            logger.info(f"{key}: {value}")

        logger.info("\n✅ 모든 테스트 성공!")

    except Exception as e:
        logger.error(f"❌ 테스트 실패: {e}", exc_info=True)
        return False

    return True

if __name__ == "__main__":
    success = test_gemini_api()
    sys.exit(0 if success else 1)