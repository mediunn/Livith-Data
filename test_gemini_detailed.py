#!/usr/bin/env python3
"""
Gemini API 상세 테스트 스크립트
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

def test_gemini_search_detailed():
    """Gemini Search 기능 상세 테스트"""

    api_key = Config.GEMINI_API_KEY
    if not api_key:
        logger.error("GEMINI_API_KEY가 설정되지 않았습니다.")
        return

    try:
        logger.info("=== Gemini API Search 상세 테스트 시작 ===\n")
        gemini = GeminiAPI(api_key)

        # 다양한 검색 쿼리 테스트
        test_queries = [
            {
                "name": "간단한 정보 요청",
                "prompt": "현재 서울의 날씨는 어떤가요?",
                "use_search": True
            },
            {
                "name": "최신 뉴스 검색",
                "prompt": "2025년 1월 최신 K-POP 뉴스를 알려주세요",
                "use_search": True
            },
            {
                "name": "콘서트 정보 검색",
                "prompt": "2025년 1월에 예정된 한국 내한 콘서트를 알려주세요. 예스24티켓이나 인터파크티켓에서 판매 중인 공연 위주로.",
                "use_search": True
            },
            {
                "name": "특정 아티스트 검색",
                "prompt": "BTS의 2025년 월드 투어 일정이 발표되었나요?",
                "use_search": True
            },
            {
                "name": "검색 없이 일반 질문",
                "prompt": "파이썬에서 리스트와 튜플의 차이점은 무엇인가요?",
                "use_search": False
            }
        ]

        for i, test in enumerate(test_queries, 1):
            logger.info(f"\n{'='*50}")
            logger.info(f"테스트 {i}: {test['name']}")
            logger.info(f"검색 사용: {test['use_search']}")
            logger.info(f"프롬프트: {test['prompt']}")
            logger.info(f"{'='*50}")

            try:
                response = gemini.query_with_search(
                    test['prompt'],
                    search_focus=test['use_search']
                )

                if response:
                    logger.info(f"✅ 응답 성공 ({len(response)} 문자)")
                    logger.info(f"응답 내용:\n{response[:1000]}...")
                    if len(response) > 1000:
                        logger.info("... (응답이 너무 길어 생략)")
                else:
                    logger.warning(f"⚠️ 빈 응답 반환")

            except Exception as e:
                logger.error(f"❌ 오류 발생: {e}")

        # JSON 응답 테스트
        logger.info(f"\n{'='*50}")
        logger.info("JSON 응답 테스트")
        logger.info(f"{'='*50}")

        json_test_prompt = """
        2025년 1월 한국 내한 콘서트 정보를 찾아서 다음 형식의 JSON으로 반환해주세요:
        {
            "concerts": [
                {
                    "artist": "아티스트명",
                    "date": "공연날짜",
                    "venue": "공연장소",
                    "status": "예정/완료"
                }
            ],
            "search_date": "검색날짜"
        }

        실제 정보가 없으면 빈 배열을 반환해주세요.
        """

        try:
            json_response = gemini.query_json(json_test_prompt, use_search=True)
            if json_response:
                logger.info(f"✅ JSON 응답 성공")
                import json
                logger.info(f"JSON 내용:\n{json.dumps(json_response, indent=2, ensure_ascii=False)}")
            else:
                logger.warning("⚠️ 빈 JSON 응답")
        except Exception as e:
            logger.error(f"❌ JSON 테스트 실패: {e}")

        logger.info("\n=== 테스트 완료 ===")

    except Exception as e:
        logger.error(f"테스트 중 오류 발생: {e}", exc_info=True)
        return False

    return True

if __name__ == "__main__":
    success = test_gemini_search_detailed()
    sys.exit(0 if success else 1)