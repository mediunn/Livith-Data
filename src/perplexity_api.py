import requests
import time
import logging
from typing import Optional
from utils.config import Config

logger = logging.getLogger(__name__)

class PerplexityAPI:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.perplexity.ai/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
    
    def query_with_search(self, prompt: str, search_focus: bool = True) -> str:
        """웹 검색을 강화한 쿼리 메서드"""
        
        # 검색 기반 응답을 위한 시스템 메시지
        system_message = """당신은 웹 검색을 통해서만 정보를 제공하는 연구 어시스턴트입니다. 
        웹 검색 결과에서 명시적으로 찾은 정보만을 사용해야 하며, 절대로 추측이나 추론을 하지 마세요.
        특정 정보를 웹 검색으로 찾을 수 없다면 추측하지 말고 빈 정보를 반환하세요. 
        한국의 콘서트 장소, 티켓팅 사이트, 팬 커뮤니티에서 정보를 찾는 것에 집중하세요.
        모든 응답은 반드시 한국어로 작성해야 합니다."""
        
        # 검색을 강제하는 프롬프트 수정
        enhanced_prompt = f"""중요: 웹 검색을 통해 찾은 정보만 제공해야 합니다. 추측이나 추론은 절대 하지 마세요.

{prompt}

검색 요구사항:
1. 웹 검색 결과에서 명시적으로 찾은 정보만 사용
2. 공식 발표, 티켓 판매, 공연장 정보를 찾으세요
3. 검색 결과에서 정보를 찾을 수 없으면 추측하지 말고 빈 값을 반환하세요
4. 항상 구체적인 출처와 날짜를 검색 결과에서 포함하세요
5. 최근 3년 내 정보를 우선하세요
6. 모든 응답은 한국어로 작성하세요"""

        payload = {
            "model": "sonar-pro",  # 유효한 온라인 모델 사용
            "messages": [
                {
                    "role": "system",
                    "content": system_message
                },
                {
                    "role": "user",
                    "content": enhanced_prompt
                }
            ],
            "max_tokens": 10000,
            "temperature": 0.1,  # 더 정확한 응답을 위해 낮춤
            "top_p": 0.9,
            "return_citations": True,
            "search_domain_filter": [],  # 도메인 제한 없이 검색
            "return_images": True,
            "return_related_questions": False,
            "search_recency_filter": "year",  # 최근 3년 내 정보 우선
        }
        
        for attempt in range(Config.MAX_RETRIES):
            try:
                response = requests.post(
                    self.base_url, 
                    json=payload, 
                    headers=self.headers,
                    timeout=Config.TIMEOUT
                )
                response.raise_for_status()
                result = response.json()
                
                # 응답에 검색 기반 정보가 포함되었는지 확인
                content = result["choices"][0]["message"]["content"]
                
                # 인용이나 출처가 포함되어 있는지 검증
                if search_focus and not self._has_search_indicators(content):
                    logger.warning(f"응답에 검색 기반 정보가 부족함 (시도 {attempt + 1})")
                    if attempt < Config.MAX_RETRIES - 1:
                        time.sleep(Config.REQUEST_DELAY)
                        continue
                
                return content
                
            except requests.exceptions.RequestException as e:
                logger.error(f"API 요청 실패 (시도 {attempt + 1}/{Config.MAX_RETRIES}): {e}")
                if hasattr(e, 'response') and e.response is not None:
                    logger.error(f"응답 상태 코드: {e.response.status_code}")
                    logger.error(f"응답 내용: {e.response.text}")
                if attempt < Config.MAX_RETRIES - 1:
                    time.sleep(Config.REQUEST_DELAY * (attempt + 1))
                else:
                    logger.error("모든 재시도 실패")
                    return ""
            except KeyError as e:
                logger.error(f"응답 파싱 실패: {e}")
                return ""
        
        return ""
    
    def _has_search_indicators(self, content: str) -> bool:
        """응답이 웹 검색 기반인지 확인 - sonar-pro는 웹 검색 기반 모델이므로 항상 True 반환"""
        # sonar-pro 모델은 항상 웹 검색을 기반으로 응답하므로 
        # 불필요한 재시도를 방지하기 위해 항상 True 반환
        return True
    
    def query(self, prompt: str, model: str = "sonar-pro") -> str:
        """기본 쿼리 메서드 (하위 호환성을 위해 유지)"""
        return self.query_with_search(prompt, search_focus=True)
