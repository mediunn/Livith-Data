"""
KOPIS API를 사용하여 공연 정보를 수집하는 API 클라이언트
"""
import logging
import re
import requests
import xml.etree.ElementTree as ET
from typing import List, Dict, Any, Optional, Set
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class KopisAPI:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "http://www.kopis.or.kr/openApi/restful"
        # 세션을 사용하여 연결 재사용 및 안정성 향상
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/xml', # XML형식으로 요청
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36' # User-Agent를 브라우저처럼 설정
        })
    
    def fetch_all_concerts(self, start_date: str = None, end_date: str = None) -> List[str]:
        # 요청한 콘서트 가져오기
        if start_date and end_date:
            logger.info(f"{start_date}~{end_date} 기간의 모든 콘서트 수집...") # 입력한 날짜 범위 안의 모든 공연 코드 수집
            all_codes = []
            for state in ["01", "02", "03"]:
                # "01"=예정, "02"=진행중, "03"=완료 모두 조회 후 가져옴
                all_codes.extend(self.fetch_concerts_in_range(start_date, end_date, state))
            unique_codes = list(set(all_codes))
            logger.info(f"총 {len(unique_codes)}개의 고유한 공연 수집")
            return unique_codes
        
        # 날짜 범위 입력 없으면 기본값 - 당일부터 6개월
        now = datetime.now()
        today = now.strftime("%Y%m%d")
        six_months_later = (now + timedelta(days=180)).strftime("%Y%m%d")

        all_codes = []
        all_codes.extend(self.fetch_concerts_in_range(today, six_months_later, "01"))  # 예정
        all_codes.extend(self.fetch_concerts_in_range(today, six_months_later, "02"))  # 진행중

        unique_codes = list(set(all_codes))
        logger.info(f"총 {len(unique_codes)}개의 고유한 공연 수집 (오늘 ~ 6개월)")
        return unique_codes
    
    def fetch_concerts_in_range(self, start_date: str, end_date: str, state: str) -> List[str]:
        # 특정 기간과 상태의 콘서트 목록 수집 
        # 한 페이지 당 최대 100개 공연 수집 가능
        page = 1
        rows = 100
        result = []
        
        while True:
            url = f"{self.base_url}/pblprfr"
            params = {
                'service': self.api_key,
                'stdate': start_date,
                'eddate': end_date,
                'rows': rows,
                'cpage': page,
                'shcate': 'CCCD', # 대중음악 장르만 수집
                'prfstate': state # 공연 상태
            }
            
            try:
                response = self.session.get(url, params=params, timeout=15)
                response.raise_for_status()
                
                root = ET.fromstring(response.text)
                items = root.findall('.//db')
                
                if not items:
                    break
                
                for item in items:
                    mt20id = item.find('mt20id') # 공연 고유 코드 추출
                    if mt20id is not None and mt20id.text:
                        result.append(mt20id.text)
                
                if len(items) < rows:
                    break
                    
                page += 1
                
            except Exception as e:
                logger.error(f"공연 목록 가져오기 실패 ({start_date}~{end_date}, state:{state}): {e}")
                break
        
        return result
    
    def get_concert_detail(self, code: str) -> Optional[Dict[str, Any]]:
        #공연의 상세 정보 수집
        url = f"{self.base_url}/pblprfr/{code}?service={self.api_key}"

        try:
            response = self.session.get(url, timeout=15)
            
            if response.status_code != 200:
                logger.error(f"공연 상세정보 HTTP 에러 (코드: {code}): {response.status_code}")
                logger.error(f"응답 내용: {response.text[:500]}")
                return None
            
            root = ET.fromstring(response.text)
            db = root.find('.//db')
            
            if db is None:
                return None

            return {
                #딕셔너리로 정리
                'code': self._get_text(db, 'mt20id'),           # 공연 고유 코드
                'title': self._get_text(db, 'prfnm'),           # 공연 제목
                'start_date': self._get_text(db, 'prfpdfrom'),  # 공연 시작일
                'end_date': self._get_text(db, 'prfpdto'),      # 공연 종료일
                'artist': self._get_text(db, 'prfcast'),        # 출연 아티스트
                'poster': self._get_text(db, 'poster'),         # 포스터 이미지 URL
                'status': self._get_text(db, 'prfstate'),       # 공연 상태 (예정/진행중/완료)
                'venue': self._clean_venue(self._get_text(db, 'fcltynm')),  # 공연장 이름
                'runtime': self._get_text(db, 'prfruntime'),    # 공연 시간
                'age': self._get_text(db, 'prfage'),            # 관람 연령
                'visit': self._get_text(db, 'visit'),           # 내한공연 여부 (Y/N)
                'festival': self._get_text(db, 'festival'),     # 페스티벌 여부 (Y/N)
                'genre': self._get_text(db, 'genrenm'),         # 장르명
                'ticket_url': self._get_text(db, 'relateurl'),  # 티켓 예매 URL
                'ticket_info': self._get_text(db, 'pcseguidance'),  # 티켓 가격 정보
                'dtguidance': self._get_text(db, 'dtguidance'),     # 공연 시간 안내
                'sty': self._get_text(db, 'sty'),                   # 공연 시놉시스/설명
                'sponsor': self._get_text(db, 'spon'),              # 후원사
                'producer': self._get_text(db, 'entrpsnm'),         # 제작사
                'producer_host': self._get_text(db, 'entrpsnmH'),   # 주최사
                'producer_plan': self._get_text(db, 'entrpsnmP'),   # 기획사
                'producer_agency': self._get_text(db, 'entrpsnmA'), # 에이전시
                'producer_sponsor': self._get_text(db, 'entrpsnmS'),# 후원사(제작)
            }
            
        except requests.RequestException as e:
            logger.error(f"공연 상세정보 요청 실패 (코드: {code}): {e}")
            return None
        except ET.ParseError as e:
            logger.error(f"공연 상세정보 XML 파싱 실패 (코드: {code}): {e}")
            return None
        except Exception as e:
            logger.error(f"공연 상세정보 처리 실패 (코드: {code}): {e}")
            return None
    
    def fetch_concert_details(
        self, 
        concert_codes: List[str], # 코드 목록
        existing_codes: Set[str] = None, 
        max_found: int = None,
        skip_filter: bool = False  # True면 내한공연 필터링 스킵, False면 내한공연만 수집
        ) -> List[Dict[str, Any]]:
        #공연 상세정보 수집 - 모든 내한공연 필터링
        result = []

        # 이미 DB에 있는 공연 코드 제외(불필요한 API 호출 방지)
        if existing_codes:
            original_count = len(concert_codes)
            concert_codes = [code for code in concert_codes if code not in existing_codes]
            excluded_count = original_count - len(concert_codes)
            if excluded_count > 0:
                logger.info(f"중복 제외: {excluded_count}개 콘서트 건너뜀 (남은 처리 대상: {len(concert_codes)}개)")

        if not concert_codes:
            logger.info("처리할 새로운 콘서트가 없습니다.")
            return result

        if skip_filter:
            log_msg = f"공연 상세정보 수집 시작: {len(concert_codes)}개 공연 처리 (필터링 스킵)"
        else:
            log_msg = f"내한공연 필터링 시작: {len(concert_codes)}개 공연 처리"
            if max_found:
                log_msg += f" (최대 {max_found}개 발견시 중단)"
        logger.info(log_msg)

        for i, code in enumerate(concert_codes, 1):
            detail = self.get_concert_detail(code)

            if detail is None:
                continue

            # 진행 상황 표시
            if i % 100 == 0:
                logger.info(f"진행: {i}/{len(concert_codes)} (수집된 공연 {len(result)}개)")

            # 필터링 스킵 모드면 바로 추가
            if skip_filter:
                result.append(detail)
                logger.info(f"✅ 공연 수집 ({len(result)}개): {detail['title']} - {detail['artist']}")
            # 내한공연 필터링
            elif self._is_visit_concert(detail):
                result.append(detail)
                logger.info(f"✅ 내한공연 발견 ({len(result)}개): {detail['title']} - {detail['artist']}")

                if max_found and len(result) >= max_found:
                    logger.info(f"🎯 최대 개수 달성! {len(result)}개 내한공연 발견")
                    return result

        logger.info(f"🏁 처리 완료: {len(concert_codes)}개 처리, {len(result)}개 공연 수집")
        return result
    
    def _is_visit_concert(self, detail: Dict[str, Any]) -> bool:
        # 내한공연 여부 확인
        return (
            detail.get('visit') == 'Y' and 
            detail.get('festival') == 'N' and
            bool(detail.get('title')) and 
            bool(detail.get('artist'))
        )
    
    def _get_text(self, element: ET.Element, tag: str) -> str:
        # XML 요소에서 텍스트 추출
        found = element.find(tag)
        return found.text if found is not None and found.text else ""
    
    def _clean_venue(self, venue: str) -> str:
        #장소 중복 괄호 제거 (예: '롤링홀 (롤링홀)' → '롤링홀')
        if not venue or '(' not in venue:
            return venue

        match = re.match(r'^(.*?)\s*\((.*?)\)\s*$', venue)
        if match:
            main = match.group(1).strip()
            bracket = match.group(2).strip()
            if main == bracket or main in bracket or bracket in main:
                return main
        return venue
    
