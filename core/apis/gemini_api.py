"""
Google AI Studio (Gemini) API 클라이언트
google-genai SDK 사용
"""
import time
import logging
import json
import textwrap
from typing import Dict, Any
from google import genai
from google.genai import types
from lib.config import Config

logger = logging.getLogger(__name__)


class GeminiAPI:
    def __init__(self, api_key: str):
        """Gemini API 클라이언트 초기화"""
        self.api_key = api_key
        self.client = genai.Client(api_key=api_key)
        self.model = 'gemini-2.5-flash' # Gemini 모델 이름
        self._search_logged = False # Google Search grounding 활성화 로그를 한 번만 출력

        self.generation_config = types.GenerateContentConfig(
            temperature=0.3, # 모험적인 정도
            top_p=0.7, # 확률 합 70%
            top_k=10, # 상위 10개
            max_output_tokens=2048,
        )

        logger.info(f"Gemini 모델 초기화 완료: {self.model}")

    def query_with_search(
            self,
            prompt: str,              # 사용자가 보내는 요청 텍스트
            search_focus: bool = True # Google Search grounding 활성화 여부
        ) -> str:

        """
        Google Search grounding을 활용한 실시간 웹 검색 쿼리
        """
        if search_focus: # Google Search grounding = True 일 경우
            system_prompt = textwrap.dedent("""\
                당신은 내한 공연 정보 검색 전문가입니다.

                중요 지침:
                - 최신 공연/콘서트 정보를 정확하게 검색하여 제공하세요.
                - 검색 결과에 기반한 사실만 답변하세요.
                - 정보를 찾을 수 없으면 명확히 알려주세요.
                - 모든 응답은 한국어로 작성하세요.""")
        else: # Google Search grounding = False 일 경우
            system_prompt = textwrap.dedent("""\
                당신은 정보를 제공하는 AI 어시스턴트입니다.

                지식 기반으로 정확한 답변을 제공하세요.
                한국어로 응답하세요.""")

        enhanced_prompt = f"{system_prompt}\n\n요청사항: {prompt}"

        # config는 재시도마다 바뀌지 않으므로 루프 밖에서 한 번만 생성
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
            config.tools = [types.Tool(google_search=types.GoogleSearch())]

        for attempt in range(Config.MAX_RETRIES):  # 재시도 루프, MAX_RETRIES 횟수만큼 시도
            try:

                response = self.client.models.generate_content(
                    model=self.model,
                    contents=enhanced_prompt,
                    config=config,
                )

                response_text = response.text  # 응답에서 텍스트 추출

                if response_text:
                    return response_text
                else:
                    logger.warning(f"빈 응답 (시도 {attempt + 1})")
                    if attempt < Config.MAX_RETRIES - 1:
                        time.sleep(Config.REQUEST_DELAY)
                        continue

            except Exception as e:
                # API 호출 중 예외 발생 시 에러로그 출력
                logger.error(f"Gemini API 요청 실패 (시도 {attempt + 1}/{Config.MAX_RETRIES}): {e}")
                if attempt < Config.MAX_RETRIES - 1:
                    time.sleep(Config.REQUEST_DELAY * (attempt + 1))
                else:
                    logger.error("모든 재시도 실패")
                    return ""

        return ""

    def query(self, prompt: str, use_search: bool = True) -> str:
        # 기본 쿼리 메서드
        return self.query_with_search(prompt, search_focus=use_search)

    def query_json(self, prompt: str, retry_on_parse_error: bool = True, use_search: bool = True) -> Dict[str, Any]:
        # JSON 응답을 파싱하여 반환하는 메서드(딕셔너리 변환)
        json_instruction = textwrap.dedent("""\
            중요: 반드시 유효한 JSON 형식으로만 응답하세요.
            - JSON 외의 설명이나 주석을 포함하지 마세요
            - 백틱(```)이나 마크다운 문법을 사용하지 마세요
            - 순수한 JSON 데이터만 반환하세요
            - Google Search로 찾은 최신 정보를 JSON으로 구성하세요""")

        json_prompt = f"{prompt}\n\n{json_instruction}"

        for attempt in range(Config.MAX_RETRIES if retry_on_parse_error else 1):  # 재시도 루프
            try:
                response = self.query(json_prompt, use_search=use_search) # Gemini한테 요청

                cleaned_response = response.strip() # 공백 제거

                #불필요한 문자 정리
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

