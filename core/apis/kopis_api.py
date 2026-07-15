"""
KOPIS API를 사용하여 공연 정보를 수집하는 API 클라이언트
"""
import time
import logging
import requests
import xml.etree.ElementTree as ET
from typing import List, Dict, Any, Optional, Set
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class KopisAPIError(Exception):
    """KOPIS API 400 에러 - 재시도 없이 스킵"""
    pass


class KopisAPI:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "http://www.kopis.or.kr/openApi/restful"
        self.request_delay = 0  # Rate limiting은 호출자가 처리
        
        # 세션을 사용하여 연결 재사용 및 안정성 향상
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/xml',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    def fetch_all_concerts(self, start_date: str = None, end_date: str = None) -> List[str]:
        """다양한 상태의 콘서트를 모두 가져오기"""
        if start_date and end_date:
            logger.info(f"{start_date}~{end_date} 기간의 모든 콘서트 수집...")
            all_codes = []
            for state in ["01", "02", "03"]:
                all_codes.extend(self.fetch_concerts_in_range(start_date, end_date, state))
            unique_codes = list(set(all_codes))
            logger.info(f"총 {len(unique_codes)}개의 고유한 공연 수집")
            return unique_codes
        
        # 날짜 범위가 없으면 기본값 사용
        now = datetime.now()
        today = now.strftime("%Y%m%d")
        one_month_ago = (now - timedelta(days=30)).strftime("%Y%m%d")
        end_of_year = datetime(now.year, 12, 31).strftime("%Y%m%d")

        all_codes = []
        all_codes.extend(self.fetch_concerts_in_range(one_month_ago, today, "03"))  # 완료
        all_codes.extend(self.fetch_concerts_in_range(one_month_ago, today, "02"))  # 진행중
        all_codes.extend(self.fetch_concerts_in_range(today, end_of_year, "01"))    # 예정

        unique_codes = list(set(all_codes))
        logger.info(f"총 {len(unique_codes)}개의 고유한 공연 수집 (과거 30일 ~ 연말)")
        return unique_codes
    
    def fetch_concerts_in_range(self, start_date: str, end_date: str, state: str) -> List[str]:
        """특정 기간과 상태의 콘서트 목록 가져오기"""
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
                'shcate': 'CCCD',  # 대중음악
                'prfstate': state
            }
            
            try:
                response = self.session.get(url, params=params, timeout=15)
                response.raise_for_status()
                
                root = ET.fromstring(response.text)
                items = root.findall('.//db')
                
                if not items:
                    break
                
                for item in items:
                    mt20id = item.find('mt20id')
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
        """단일 공연의 상세 정보를 가져오기"""
        url = f"{self.base_url}/pblprfr/{code}?service={self.api_key}"

        try:
            if self.request_delay > 0:
                time.sleep(self.request_delay)
            response = self.session.get(url, timeout=15)
            
            if response.status_code == 400:
                logger.debug(f"공연 코드 없음 (코드: {code}): 400")
                raise KopisAPIError(f"400: {code}")
            if response.status_code != 200:
                logger.error(f"공연 상세정보 HTTP 에러 (코드: {code}): {response.status_code}")
                return None
            
            root = ET.fromstring(response.text)
            db = root.find('.//db')
            
            if db is None:
                return None
            
            return {
                'code': self._get_text(db, 'mt20id'),
                'title': self._get_text(db, 'prfnm'),
                'start_date': self._get_text(db, 'prfpdfrom'),
                'end_date': self._get_text(db, 'prfpdto'),
                'artist': self._get_text(db, 'prfcast'),
                'poster': self._get_text(db, 'poster'),
                'status': self._get_text(db, 'prfstate'),
                'venue': self._clean_venue(self._get_text(db, 'fcltynm')),
                'runtime': self._get_text(db, 'prfruntime'),
                'age': self._get_text(db, 'prfage'),
                'visit': self._get_text(db, 'visit'),
                'festival': self._get_text(db, 'festival'),
                'genre': self._get_text(db, 'genrenm'),
                'ticket_url': self._get_text(db, 'relateurl'),
                'ticket_info': self._get_text(db, 'pcseguidance'),
                'dtguidance': self._get_text(db, 'dtguidance'),
                'sty': self._get_text(db, 'sty'),
                'sponsor': self._get_text(db, 'spon'),
                'producer': self._get_text(db, 'entrpsnm'),
                'producer_host': self._get_text(db, 'entrpsnmH'),
                'producer_plan': self._get_text(db, 'entrpsnmP'),
                'producer_agency': self._get_text(db, 'entrpsnmA'),
                'producer_sponsor': self._get_text(db, 'entrpsnmS'),
            }
            
        except KopisAPIError:
            raise
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
        concert_codes: List[str], 
        existing_codes: Set[str] = None, 
        max_found: int = None,
        skip_filter: bool = False  # 추가: 내한공연 필터링 스킵 여부
        ) -> List[Dict[str, Any]]:
        """공연 상세정보 가져오기 - 모든 내한공연 필터링"""
        result = []
    
    # 기존 코드 제외
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
        """내한공연 여부 확인"""
        return (
            detail.get('visit') == 'Y' and 
            detail.get('festival') == 'N' and
            bool(detail.get('title')) and 
            bool(detail.get('artist'))
        )
    
    def _get_text(self, element: ET.Element, tag: str) -> str:
        """XML 요소에서 텍스트 추출"""
        found = element.find(tag)
        return found.text if found is not None and found.text else ""
    
    def _clean_venue(self, venue: str) -> str:
        """장소 중복 괄호 제거: 'A (B) (A (B))' → 'A (B)'"""
        if not venue or ' (' not in venue:
            return venue
        
        # 첫 번째 완전한 괄호까지만 추출
        depth = 0
        end_idx = len(venue)
        
        for i, char in enumerate(venue):
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
                if depth == 0:
                    # 첫 번째 괄호 닫힘 이후에 또 같은 내용이 반복되는지 확인
                    first_part = venue[:i+1].strip()
                    remaining = venue[i+1:].strip()
                    
                    # 남은 부분이 괄호로 시작하고 첫 부분의 내용을 포함하면 중복
                    if remaining.startswith('(') and first_part.split(' (')[0] in remaining:
                        return first_part
                    break
        
        return venue
    
    def _map_status_to_enum(self, status: str) -> str:
        """KOPIS 상태 코드를 enum으로 매핑"""
        status_mapping = {
            '01': 'UPCOMING',
            '02': 'ONGOING',
            '03': 'COMPLETED'
        }
        return status_mapping.get(status, 'UNKNOWN')