import requests
import xml.etree.ElementTree as ET
import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta
from config import Config

logger = logging.getLogger(__name__)

class KopisAPI:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "http://www.kopis.or.kr/openApi/restful"
    
    def fetch_all_concerts(self) -> List[str]:
        """다양한 상태의 콘서트를 모두 가져오기 (최대 50개 제한)"""
        now = datetime.now()
        today = now.strftime("%Y%m%d")
        yesterday = (now - timedelta(days=1)).strftime("%Y%m%d")
        one_month_ago = (now - timedelta(days=30)).strftime("%Y%m%d")
        
        all_codes = []
        max_concerts = 1000  # 제한 대폭 확대
        
        # 1. 공연 중 (오늘)
        logger.info("공연 중인 콘서트 수집...")
        ongoing_codes = self.fetch_concerts_in_range(today, today, "02")
        all_codes.extend(ongoing_codes[:max_concerts])
        logger.info(f"공연 중: {len(ongoing_codes)}개 (제한: {min(len(ongoing_codes), max_concerts)}개)")
        
        if len(all_codes) >= max_concerts:
            all_codes = all_codes[:max_concerts]
            logger.info(f"최대 제한 도달: {len(all_codes)}개")
            return all_codes
        
        # 2. 공연 완료 (한 달 전~어제)
        logger.info("최근 완료된 콘서트 수집...")
        remaining_slots = max_concerts - len(all_codes)
        completed_codes = self.fetch_concerts_in_range(one_month_ago, yesterday, "03")
        all_codes.extend(completed_codes[:remaining_slots])
        logger.info(f"최근 완료: {len(completed_codes)}개 (제한: {min(len(completed_codes), remaining_slots)}개)")
        
        if len(all_codes) >= max_concerts:
            all_codes = all_codes[:max_concerts]
            logger.info(f"최대 제한 도달: {len(all_codes)}개")
            return all_codes
        
        # 3. 공연 예정 (내일부터 3개월)
        logger.info("예정된 콘서트 수집...")
        future_codes = []
        start_date = now + timedelta(days=1)
        remaining_slots = max_concerts - len(all_codes)
        
        for i in range(3):  # 3개월치
            if len(future_codes) >= remaining_slots:
                break
                
            month_start = start_date + timedelta(days=30*i)
            month_end = start_date + timedelta(days=30*(i+1) - 1)
            
            month_start_str = month_start.strftime("%Y%m%d")
            month_end_str = month_end.strftime("%Y%m%d")
            
            monthly_codes = self.fetch_concerts_in_range(month_start_str, month_end_str, "01")
            available_slots = remaining_slots - len(future_codes)
            future_codes.extend(monthly_codes[:available_slots])
        
        all_codes.extend(future_codes)
        logger.info(f"예정: {len(future_codes)}개")
        
        # 중복 제거 후 최대 1000개로 제한
        unique_codes = list(set(all_codes))[:max_concerts]
        logger.info(f"총 {len(unique_codes)}개의 고유한 공연 (최대 {max_concerts}개 제한)")
        
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
                'prfstate': state  # 01: 공연예정, 02: 공연중, 03: 공연완료
            }
            
            try:
                response = requests.get(url, params=params, headers={'Accept': 'application/xml'})
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
    
    def fetch_concert_details(self, concert_codes: List[str], max_found: int = None) -> List[Dict[str, Any]]:
        """공연 상세정보 가져오기 - 모든 내한공연 필터링"""
        result = []
        processed = 0
        batch_size = 50
        
        if max_found:
            logger.info(f"내한공연 필터링 시작: {len(concert_codes)}개 공연 처리 (최대 {max_found}개 발견시 중단)")
        else:
            logger.info(f"모든 내한공연 필터링 시작: {len(concert_codes)}개 공연 전체 처리")
        
        # 배치 단위로 처리
        for batch_start in range(0, len(concert_codes), batch_size):
            batch_end = min(batch_start + batch_size, len(concert_codes))
            batch_codes = concert_codes[batch_start:batch_end]
            
            logger.info(f"배치 처리: {batch_start+1}-{batch_end}/{len(concert_codes)} (현재 내한공연: {len(result)}개)")
            
            for i, code in enumerate(batch_codes):
                current_index = batch_start + i + 1
                
                url = f"{self.base_url}/pblprfr/{code}"
                params = {'service': self.api_key}
                
                try:
                    response = requests.get(url, params=params, headers={'Accept': 'application/xml'})
                    response.raise_for_status()
                    
                    root = ET.fromstring(response.text)
                    db = root.find('.//db')
                    
                    if db is not None:
                        processed += 1
                        
                        # 모든 필드 추출 (디버깅을 위해 모든 가능한 필드 시도)
                        concert_data = {
                            'code': self._get_text(db, 'mt20id'),
                            'title': self._get_text(db, 'prfnm'),
                            'start_date': self._get_text(db, 'prfpdfrom'),
                            'end_date': self._get_text(db, 'prfpdto'),
                            'artist': self._get_text(db, 'prfcast'),
                            'poster': self._get_text(db, 'poster'),
                            'status': self._get_text(db, 'prfstate'),
                            'venue': self._get_text(db, 'fcltynm'),
                            'runtime': self._get_text(db, 'prfruntime'),
                            'age': self._get_text(db, 'prfage'),
                            'visit': self._get_text(db, 'visit'),
                            'festival': self._get_text(db, 'festival'),
                            'genre': self._get_text(db, 'genrenm'),
                            # 티켓 관련 가능한 필드들 시도
                            'ticket_url': self._get_text(db, 'relateurl'),
                            'ticket_url2': self._get_text(db, 'dtguidance'),
                            'ticket_url3': self._get_text(db, 'sty'),
                            'ticket_info': self._get_text(db, 'pcseguidance'),
                            'sponsor': self._get_text(db, 'spon'),
                            'producer': self._get_text(db, 'entrpsnm'),
                            'entrps': self._get_text(db, 'entrpsnmH'),
                            'producer2': self._get_text(db, 'entrpsnmP'),
                            'producer3': self._get_text(db, 'entrpsnmA'),
                            'producer4': self._get_text(db, 'entrpsnmH'),
                            'producer5': self._get_text(db, 'entrpsnmS'),
                        }
                        
                        # 디버깅 로그 제거됨
                        
                        # 내한공연 필터링 조건
                        if (concert_data['visit'] == 'Y' and 
                            concert_data['festival'] == 'N' and
                            concert_data['title'] and 
                            concert_data['artist']):
                            
                            result.append(concert_data)
                            logger.info(f"✅ 내한공연 발견 ({len(result)}개): {concert_data['title']} - {concert_data['artist']}")
                            
                            # 최대 개수 달성시 조기 종료
                            if max_found and len(result) >= max_found:
                                logger.info(f"🎯 최대 개수 달성! {len(result)}개 내한공연 발견 (전체 {processed}개 중)")
                                return result
                        else:
                            logger.debug(f"필터링됨: {concert_data['title']} (visit:{concert_data['visit']}, festival:{concert_data['festival']})")
                        
                except Exception as e:
                    logger.error(f"공연 상세정보 가져오기 실패 (코드: {code}): {e}")
                    continue
            
            # 배치 완료 후 진행 상황 출력
            efficiency = (len(result) / processed) * 100 if processed > 0 else 0
            logger.info(f"📊 배치 완료: 처리 {processed}개, 내한공연 {len(result)}개 (효율: {efficiency:.1f}%)")
        
        logger.info(f"🏁 전체 필터링 완료: {processed}개 공연 처리, 총 {len(result)}개 내한공연 발견")
        return result
    
    def _get_text(self, element: ET.Element, tag: str) -> str:
        """XML 요소에서 텍스트 추출"""
        found = element.find(tag)
        return found.text if found is not None and found.text else ""
    
    def _map_status_to_enum(self, status: str) -> str:
        """KOPIS 상태 코드를 enum으로 매핑"""
        status_mapping = {
            '01': 'UPCOMING',    # 공연예정
            '02': 'ONGOING',     # 공연중
            '03': 'COMPLETED'    # 공연완료
        }
        return status_mapping.get(status, 'UNKNOWN')
