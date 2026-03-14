"""
Google AI Studio (Gemini) API 클라이언트
google-genai SDK 사용
"""
import time
import logging
import json
from typing import Dict, Any, List
from google import genai
from google.genai import types
from lib.config import Config

logger = logging.getLogger(__name__)


class GeminiAPI:
    def __init__(self, api_key: str):
        """Gemini API 클라이언트 초기화"""
        self.api_key = api_key
        self.client = genai.Client(api_key=api_key)
        self.model = 'gemini-2.5-flash'
        self._search_logged = False

        self.generation_config = types.GenerateContentConfig(
            temperature=0.7,
            top_p=0.8,
            top_k=20,
            max_output_tokens=4096,
        )

        logger.info(f"Gemini 모델 초기화 완료: {self.model}")

    def query_with_search(self, prompt: str, search_focus: bool = True, urls: List[str] = None, context: str = None) -> str:
        """
        Google Search grounding을 활용한 실시간 웹 검색 쿼리
        """
        if search_focus:
            system_prompt = """당신은 실시간 정보 검색 전문가입니다.

중요 지침:
1. 최신 정보를 정확하게 검색하여 제공하세요.
2. 검색 결과에 기반한 사실만 답변하세요.
3. 정보를 찾을 수 없으면 명확히 알려주세요.
4. 모든 응답은 한국어로 작성하세요."""
        else:
            system_prompt = """당신은 유용한 정보를 제공하는 AI 어시스턴트입니다.

지식 기반으로 정확하고 도움이 되는 답변을 제공하세요.
한국어로 응답하세요."""

        url_context = ""
        if urls:
            url_context = f"\n\n참조 URL:\n" + "\n".join(f"- {url}" for url in urls)

        enhanced_prompt = f"{system_prompt}\n\n요청사항: {prompt}{url_context}"

        for attempt in range(Config.MAX_RETRIES):
            try:
                config = types.GenerateContentConfig(
                    temperature=self.generation_config.temperature,
                    top_p=self.generation_config.top_p,
                    top_k=self.generation_config.top_k,
                    max_output_tokens=self.generation_config.max_output_tokens,
                )

                if search_focus:
                    if not self._search_logged:
                        logger.info("Google Search grounding 활성화")
                        self._search_logged = True
                    if context:
                        logger.debug(f"검색 중: {context}")
                    config.tools = [types.Tool(google_search=types.GoogleSearch())]

                response = self.client.models.generate_content(
                    model=self.model,
                    contents=enhanced_prompt,
                    config=config,
                )

                response_text = response.text if response.text else None

                if response_text:
                    return response_text
                else:
                    logger.warning(f"빈 응답 (시도 {attempt + 1})")
                    if attempt < Config.MAX_RETRIES - 1:
                        time.sleep(Config.REQUEST_DELAY)
                        continue

            except Exception as e:
                logger.error(f"Gemini API 요청 실패 (시도 {attempt + 1}/{Config.MAX_RETRIES}): {e}")
                if attempt < Config.MAX_RETRIES - 1:
                    time.sleep(Config.REQUEST_DELAY * (attempt + 1))
                else:
                    logger.error("모든 재시도 실패")
                    return ""

        return ""

    def query(self, prompt: str, use_search: bool = True, urls: List[str] = None) -> str:
        """기본 쿼리 메서드"""
        return self.query_with_search(prompt, search_focus=use_search, urls=urls)

    def query_json(self, prompt: str, retry_on_parse_error: bool = True, use_search: bool = True) -> Dict[str, Any]:
        """JSON 응답을 파싱하여 반환하는 메서드"""
        json_prompt = f"""{prompt}

중요: 반드시 유효한 JSON 형식으로만 응답하세요.
- JSON 외의 설명이나 주석을 포함하지 마세요
- 백틱(```)이나 마크다운 문법을 사용하지 마세요
- 순수한 JSON 데이터만 반환하세요
- Google Search로 찾은 최신 정보를 JSON으로 구성하세요"""

        for attempt in range(Config.MAX_RETRIES if retry_on_parse_error else 1):
            try:
                response = self.query(json_prompt, use_search=use_search)

                cleaned_response = response.strip()

                if "```" in cleaned_response:
                    if "```json" in cleaned_response:
                        start_marker = "```json"
                    else:
                        start_marker = "```"

                    start_idx = cleaned_response.find(start_marker) + len(start_marker)
                    end_idx = cleaned_response.find("```", start_idx)

                    if end_idx != -1:
                        cleaned_response = cleaned_response[start_idx:end_idx].strip()
                    else:
                        cleaned_response = cleaned_response[start_idx:].strip()

                cleaned_response = cleaned_response.replace('\\n', '\n').replace('\\"', '"')

                if cleaned_response and not cleaned_response.endswith((']', '}')):
                    if cleaned_response.startswith('[') and not cleaned_response.endswith(']'):
                        cleaned_response += ']'
                    elif cleaned_response.startswith('{') and not cleaned_response.endswith('}'):
                        cleaned_response += '}'

                data = json.loads(cleaned_response)
                return data

            except json.JSONDecodeError as e:
                logger.warning(f"JSON 파싱 실패 (시도 {attempt + 1}): {e}")
                if attempt < Config.MAX_RETRIES - 1 and retry_on_parse_error:
                    time.sleep(Config.REQUEST_DELAY)
                    continue
                else:
                    logger.error(f"JSON 파싱 최종 실패. 원본 응답:\n{response}")
                    return {}
            except Exception as e:
                logger.error(f"예상치 못한 오류: {e}")
                return {}

        return {}

    def batch_query(self, prompts: list, delay_between_queries: float = 1.0, use_search: bool = True) -> list:
        """여러 프롬프트를 순차적으로 처리"""
        results = []
        for i, prompt in enumerate(prompts):
            logger.info(f"배치 쿼리 진행 중: {i+1}/{len(prompts)}")
            result = self.query(prompt, use_search=use_search)
            results.append(result)

            if i < len(prompts) - 1:
                time.sleep(delay_between_queries)

        return results

    def query_with_url_context(self, prompt: str, urls: List[str], use_search: bool = True) -> str:
        """특정 URL을 컨텍스트로 제공하여 검색하는 메서드"""
        url_context_prompt = f"""{prompt}

다음 URL들을 참조하여 정보를 찾아주세요:
{chr(10).join(f'- {url}' for url in urls)}

이 URL들의 내용과 Google Search 결과를 종합하여 답변해주세요."""

        return self.query(url_context_prompt, use_search=use_search, urls=urls)

    def get_model_info(self) -> Dict[str, str]:
        """현재 사용 중인 모델 정보 반환"""
        return {
            "model": self.model,
            "search_available": True,
            "api_status": "active"
        }
