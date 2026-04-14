"""
Perplexity AI API 클라이언트
현재 미사용 — 대체: gemini_api.py
"""
import requests
import time
import logging
import json
import re
from typing import Dict, Any

from lib.config import Config
from lib.prompts import APIPrompts

logger = logging.getLogger(__name__)


def _clean_control_chars(s: str) -> str:
    """문자열에서 제어 문자를 제거 (더욱 강력한 버전)"""
    # Printable ASCII, Hangul, and common whitespace characters are allowed.
    return re.sub(r'[^ -~가-힣\n\r\t]', '', s)


class PerplexityAPI:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.perplexity.ai/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
    
    def query_with_search(self, prompt: str) -> str:
        """웹 검색을 강화한 쿼리 메서드"""
        
        # 중앙화된 프롬프트 사용
        system_message = APIPrompts.get_perplexity_system_message()
        
        # 검색을 강제하는 프롬프트 수정
        enhanced_prompt = APIPrompts.get_perplexity_enhanced_prompt(prompt)

        payload = {
            "model": "sonar",  # 무료 모델
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
            "max_tokens": 1000,
            "temperature": 0.1,
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
                
                content = result["choices"][0]["message"]["content"]
                
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
    
    def query(self, prompt: str) -> str:
        #기본 쿼리 메서드
        return self.query_with_search(prompt)

    def query_json(self, prompt: str, retry_on_parse_error: bool = True) -> Dict[str, Any]:
        """JSON 응답을 파싱하여 반환하는 메서드"""
        json_prompt = f"""{prompt}

중요: 반드시 유효한 JSON 형식으로만 응답하세요.
- JSON 외의 설명이나 주석을 포함하지 마세요
- 백틱(```)이나 마크다운 문법을 사용하지 마세요
- 순수한 JSON 데이터만 반환하세요"""

        for attempt in range(Config.MAX_RETRIES if retry_on_parse_error else 1):
            try:
                response = self.query(json_prompt)
                
                if not response:
                    return {}
                
                cleaned_response = response.strip()
                
                if "```" in cleaned_response:
                    if "```json" in cleaned_response:
                        start_marker = "```json"
                    else:
                        start_marker = "```"
                    
                    start_idx = cleaned_response.find(start_marker) + len(start_marker)
                    end_idx = cleaned_response.rfind("```")
                    
                    if end_idx > start_idx:
                        cleaned_response = cleaned_response[start_idx:end_idx].strip()
                    else:
                        cleaned_response = cleaned_response[start_idx:].strip()
                
                cleaned_response = _clean_control_chars(cleaned_response)

                # 인코딩 문제 해결 시도
                try:
                    cleaned_response = cleaned_response.encode('latin-1').decode('utf-8', 'ignore')
                except Exception:
                    pass # 이미 올바른 형식이면 무시
                
                logger.debug(f"정리된 JSON: {cleaned_response[:500]}...")
                
                data = json.loads(cleaned_response)
                return data
                
            except json.JSONDecodeError as e:
                logger.warning(f"JSON 파싱 실패 (시도 {attempt + 1}): {e}")
                if attempt < Config.MAX_RETRIES - 1 and retry_on_parse_error:
                    logger.info("재시도 중...")
                    time.sleep(Config.REQUEST_DELAY)
                    continue
                else:
                    logger.error(f"JSON 파싱 최종 실패. 원본 응답:\n{response}")
                    return {}
            except Exception as e:
                logger.error(f"예상치 못한 오류: {e}")
                return {}
        
        return {}