import requests
import time
import logging
from typing import Optional
from config import Config

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
        system_message = """You are a research assistant that MUST search the web for current, accurate information. 
        Always base your responses on real, verifiable data from reliable sources. 
        Do not generate fictional or speculative information. 
        When providing structured data, ensure all details are factually accurate and sourced from recent web searches."""
        
        # 검색을 강제하는 프롬프트 수정
        enhanced_prompt = f"""IMPORTANT: Search the web for current, accurate information before responding.

{prompt}

Please search for the most recent and accurate information available online. Include specific dates, venues, and verifiable details. If certain information is not available through web search, clearly state that the information could not be verified rather than speculating."""

        payload = {
            "model": "llama-3.1-sonar-large-128k-online",  # 더 강력한 온라인 모델 사용
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
            "max_tokens": 4000,
            "temperature": 0.1,  # 더 정확한 응답을 위해 낮춤
            "top_p": 0.9,
            "return_citations": True,
            "search_domain_filter": [],  # 모든 도메인에서 검색 허용
            "return_images": False,
            "return_related_questions": False,
            "search_recency_filter": "month"  # 최근 1개월 내 정보 우선
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
        """응답이 웹 검색 기반인지 확인"""
        search_indicators = [
            "according to", "based on", "reported", "announced", 
            "confirmed", "official", "recent", "latest", "2024", "2025",
            "source", "website", "news", "press release"
        ]
        
        content_lower = content.lower()
        return any(indicator in content_lower for indicator in search_indicators)
    
    def query(self, prompt: str, model: str = "llama-3.1-sonar-large-128k-online") -> str:
        """기본 쿼리 메서드 (하위 호환성을 위해 유지)"""
        return self.query_with_search(prompt, search_focus=True)
