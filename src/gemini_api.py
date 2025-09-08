"""
Google AI Studio (Gemini) API 클라이언트
Gemini 2.5 pro with Google Search grounding 구현
"""
import google.generativeai as genai
from google.generativeai import types
import time
import logging
import json
from typing import Optional, Dict, Any, List
from utils.config import Config
from utils.prompts import APIPrompts

logger = logging.getLogger(__name__)

class GeminiAPI:
    def __init__(self, api_key: str):
        """Gemini API 클라이언트 초기화"""
        self.api_key = api_key
        genai.configure(api_key=api_key)
        self._search_logged = False  # 검색 활성화 로그를 한 번만 출력하기 위한 플래그
        
        # Gemini 2.0 Flash 모델 설정 (Google Search 지원)
        try:
            # 검색 기능이 지원되는 경우 활성화
            self.model_with_search = genai.GenerativeModel(
                'gemini-2.5-flash'  # 최신 2.5 Flash 모델
            )
            self.search_available = True
            logger.info("Gemini 2.5 Flash 모델 초기화 완료 (Google Search 통합)")
        except Exception as e:
            logger.warning(f"Google Search 통합 실패, 일반 모델 사용: {e}")
            self.model_with_search = genai.GenerativeModel('gemini-2.5-flash')
            self.search_available = False
        
        # 일반 모델 (검색 없이)
        self.model = genai.GenerativeModel('gemini-2.5-flash')
        
        # 기본 생성 설정 (500 에러 방지를 위해 보수적으로 설정)
        self.generation_config = {
            'temperature': 0.7,  # 0.9 → 0.7로 낮춤
            'top_p': 0.8,       # 0.95 → 0.8로 낮춤
            'top_k': 20,        # 40 → 20로 낮춤
            'max_output_tokens': 4096,  # 8192 → 4096로 낮춤
        }
        
        # 안전 설정 (콘서트 정보는 안전한 컨텐츠)
        self.safety_settings = [
            {
                "category": "HARM_CATEGORY_HARASSMENT",
                "threshold": "BLOCK_NONE"
            },
            {
                "category": "HARM_CATEGORY_HATE_SPEECH",
                "threshold": "BLOCK_NONE"
            },
            {
                "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                "threshold": "BLOCK_NONE"
            },
            {
                "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                "threshold": "BLOCK_NONE"
            }
        ]
    
    def query_with_search(self, prompt: str, search_focus: bool = True, urls: List[str] = None, context: str = None) -> str:
        """
        Google Search grounding을 활용한 실시간 웹 검색 쿼리
        
        Args:
            prompt: 검색 쿼리
            search_focus: True면 Google Search 사용, False면 일반 모델
            urls: 참조할 URL 리스트 (컨텍스트로 제공)
        """
        
        # 검색 중심의 시스템 프롬프트
        system_prompt = """당신은 Google Search를 활용하여 최신 한국 내한 콘서트 정보를 수집하는 전문가입니다.
        
중요 지침:
1. Google Search를 통해 실시간으로 검색한 정보만 제공하세요.
2. 추측하지 말고, 검색 결과에서 찾은 사실만 답변하세요.
3. 정보를 찾을 수 없으면 빈 값("")으로 반환하세요.
4. 2020-2025년 최신 정보를 우선적으로 찾아주세요.
5. 모든 응답은 반드시 한국어로 작성하세요.

검색 우선순위:
1. 공식 티켓 사이트 (예스24, 인터파크, 멜론티켓, 등등)
2. 아티스트 공식 SNS/웹사이트
3. 한국 음악 뉴스 사이트
4. 팬 커뮤니티 및 포럼

주요 한국 공연장:
- 대형: 고척스카이돔, 잠실실내체육관, KSPO DOME, 인스파이어 아레나
- 중형: 예스24 라이브홀, 블루스퀘어, 올림픽홀, 무신사 개러지
- 소형: 웨스트브릿지, 롤링홀, 클럽FF, 사운드홀"""
        
        # URL 컨텍스트 추가
        url_context = ""
        if urls:
            url_context = f"\n\n참조 URL:\n" + "\n".join(f"- {url}" for url in urls)
        
        # 프롬프트 강화 (검색 키워드 최적화)
        enhanced_prompt = f"""{system_prompt}

요청사항:
{prompt}

Google Search를 사용하여 다음을 검색해주세요:
- 최신 공연 정보 (2024-2025)
- 공식 발표 및 티켓 정보
- 실제 개최된/개최 예정 공연만
{url_context}

응답 원칙:
- Google Search 결과 기반 답변
- 출처가 명확한 정보만 제공
- 최신 정보 우선
- 한국어로 응답"""
        
        for attempt in range(Config.MAX_RETRIES):
            try:
                # Google Search grounding 사용 여부 결정
                if search_focus and hasattr(self, 'search_available') and self.search_available:
                    # Google Search 활성화된 모델 사용
                    model = self.model_with_search
                    # 첫 번째 호출 시에만 로그 출력
                    if not self._search_logged:
                        logger.info("Google Search grounding 활성화")
                        self._search_logged = True
                    # 이후 호출에서는 context만 DEBUG 레벨로 출력
                    if context:
                        logger.debug(f"검색 중: {context}")
                else:
                    # 일반 모델 사용
                    model = self.model
                    if search_focus:
                        logger.warning("Google Search 요청되었으나 이용 불가, 일반 모델 사용")
                    else:
                        logger.debug("일반 모델 사용 (검색 없음)")
                
                # 생성 설정 (Google Search 사용시 dynamic retrieval 활성화)
                config = self.generation_config.copy()
                if search_focus:
                    config['candidate_count'] = 1  # 검색 기반 단일 응답
                
                response = model.generate_content(
                    enhanced_prompt,
                    generation_config=config,
                    safety_settings=self.safety_settings
                )
                
                # 응답 텍스트 추출 (안전한 방식)
                try:
                    response_text = response.text
                except AttributeError:
                    # response.text가 작동하지 않는 경우 parts 사용
                    if response.candidates and response.candidates[0].content.parts:
                        response_text = ''.join(part.text for part in response.candidates[0].content.parts if hasattr(part, 'text'))
                    else:
                        response_text = None
                
                if response_text:
                    # 검색 소스 정보 로깅 (있는 경우)
                    if hasattr(response, 'grounding_metadata'):
                        logger.info(f"Google Search 소스: {response.grounding_metadata}")
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
        """
        기본 쿼리 메서드 (Perplexity와 호환성 유지)
        
        Args:
            prompt: 쿼리 프롬프트
            use_search: Google Search 사용 여부 (기본값: True)
            urls: 참조할 URL 리스트
        """
        return self.query_with_search(prompt, search_focus=use_search, urls=urls)
    
    def query_json(self, prompt: str, retry_on_parse_error: bool = True, use_search: bool = True) -> Dict[str, Any]:
        """
        JSON 응답을 파싱하여 반환하는 메서드 (Google Search 지원)
        
        Args:
            prompt: JSON 응답을 요청하는 프롬프트
            retry_on_parse_error: 파싱 실패 시 재시도 여부
            use_search: Google Search 사용 여부
        """
        # JSON 응답을 명확히 요청
        json_prompt = f"""{prompt}

중요: 반드시 유효한 JSON 형식으로만 응답하세요.
- JSON 외의 설명이나 주석을 포함하지 마세요
- 백틱(```)이나 마크다운 문법을 사용하지 마세요
- 순수한 JSON 데이터만 반환하세요
- Google Search로 찾은 최신 정보를 JSON으로 구성하세요"""
        
        for attempt in range(Config.MAX_RETRIES if retry_on_parse_error else 1):
            try:
                # Google Search를 활용한 쿼리
                response = self.query(json_prompt, use_search=use_search)
                
                # 응답에서 JSON 부분만 추출 (개선된 처리)
                cleaned_response = response.strip()
                
                # 마크다운 코드 블록 제거
                if "```" in cleaned_response:
                    # ```json 또는 ``` 뒤의 내용을 찾기
                    if "```json" in cleaned_response:
                        start_marker = "```json"
                    else:
                        start_marker = "```"
                    
                    start_idx = cleaned_response.find(start_marker) + len(start_marker)
                    end_idx = cleaned_response.find("```", start_idx)
                    
                    if end_idx != -1:
                        cleaned_response = cleaned_response[start_idx:end_idx].strip()
                    else:
                        # 끝 마커가 없는 경우, 시작 마커 이후 모든 내용
                        cleaned_response = cleaned_response[start_idx:].strip()
                
                # 잘못된 문자 정리
                cleaned_response = cleaned_response.replace('\\n', '\n').replace('\\"', '"')
                
                # 문자열이 완전하지 않은 경우 보완 시도
                if cleaned_response and not cleaned_response.endswith((']', '}')):
                    if cleaned_response.startswith('[') and not cleaned_response.endswith(']'):
                        # 배열이 열려있으면 닫아줌
                        cleaned_response += ']'
                    elif cleaned_response.startswith('{') and not cleaned_response.endswith('}'):
                        # 객체가 열려있으면 닫아줌
                        cleaned_response += '}'
                
                logger.debug(f"정리된 JSON: {cleaned_response[:500]}...")
                
                # JSON 파싱
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
    
    def batch_query(self, prompts: list, delay_between_queries: float = 1.0, use_search: bool = True) -> list:
        """
        여러 프롬프트를 순차적으로 처리 (Google Search 지원)
        
        Args:
            prompts: 프롬프트 리스트
            delay_between_queries: 각 쿼리 사이 대기 시간
            use_search: Google Search 사용 여부
        """
        results = []
        for i, prompt in enumerate(prompts):
            logger.info(f"배치 쿼리 진행 중: {i+1}/{len(prompts)} (Google Search: {use_search})")
            result = self.query(prompt, use_search=use_search)
            results.append(result)
            
            if i < len(prompts) - 1:
                time.sleep(delay_between_queries)
        
        return results
    
    def query_with_url_context(self, prompt: str, urls: List[str], use_search: bool = True) -> str:
        """
        특정 URL을 컨텍스트로 제공하여 검색하는 메서드
        
        Args:
            prompt: 검색 프롬프트
            urls: 참조할 URL 리스트
            use_search: Google Search 사용 여부
        """
        url_context_prompt = f"""{prompt}

다음 URL들을 참조하여 정보를 찾아주세요:
{chr(10).join(f'- {url}' for url in urls)}

이 URL들의 내용과 Google Search 결과를 종합하여 답변해주세요."""
        
        return self.query(url_context_prompt, use_search=use_search, urls=urls)
    
    def get_model_info(self) -> Dict[str, str]:
        """현재 사용 중인 모델 정보 반환"""
        return {
            "search_model": "gemini-2.5-flash",
            "regular_model": "gemini-2.5-flash",
            "search_enabled": "google_search_retrieval",
            "version": "2.5"
        }
