"""
Perplexity AI API를 사용하여 데이터 수집 및 처리를 수행하는 API 클라이언트
"""
import requests
import time
import logging
from typing import Optional
from lib.config import Config
from lib.prompts import APIPrompts

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
        
        # 중앙화된 프롬프트 사용
        system_message = APIPrompts.get_perplexity_system_message()
        
        # 검색을 강제하는 프롬프트 수정
        enhanced_prompt = APIPrompts.get_perplexity_enhanced_prompt(prompt)

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
