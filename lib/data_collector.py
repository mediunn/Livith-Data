"""
데이터 수집 핵심 로직
"""
import logging
import time
from typing import Dict, Any, Optional
from lib.data_models import Concert, Artist
from lib.config import Config

logger = logging.getLogger(__name__)


class DataCollector:
    def __init__(self, api_client):
        self.api = api_client
    
    def collect_concert_basic_info(self, kopis_data: Dict[str, Any]) -> Concert:
        """KOPIS 데이터를 기반으로 기본 콘서트 정보 생성"""
        return Concert(
            title=kopis_data.get('title', ''),
            artist=kopis_data.get('artist', ''),
            start_date=self._format_date(kopis_data.get('start_date')),
            end_date=self._format_date(kopis_data.get('end_date')),
            venue=kopis_data.get('venue', ''),
            code=kopis_data.get('code', ''),
            img_url=kopis_data.get('poster_url', ''),
            status=self._determine_status(
                kopis_data.get('start_date'),
                kopis_data.get('end_date')
            )
        )
    
    def enhance_concert_data(self, concert: Concert) -> Concert:
        """AI API를 사용하여 콘서트 정보 보강"""
        try:
            # 티켓 정보 수집
            ticket_info = self._collect_ticket_info(concert.title, concert.artist)
            if ticket_info:
                concert.ticket_url = ticket_info.get('url', '')
                concert.ticket_site = ticket_info.get('site', '')
            
            # 추가 정보 수집
            additional_info = self._collect_additional_info(concert.title, concert.artist)
            if additional_info:
                concert.introduction = additional_info.get('introduction', '')
                concert.label = additional_info.get('label', '')
            
            return concert
            
        except Exception as e:
            logger.error(f"콘서트 정보 보강 실패: {e}")
            return concert
    
    def collect_artist_info(self, artist_name: str) -> Optional[Artist]:
        """아티스트 정보 수집"""
        try:
            info = self._collect_artist_basic_info(artist_name)
            if not info:
                return None
            
            return Artist(
                artist=info.get('name', artist_name),
                debut_date=info.get('debut_date', ''),
                nationality=info.get('nationality', ''),
                group_type=info.get('group_type', ''),
                introduction=info.get('introduction', ''),
                social_media=info.get('social_media', ''),
                keywords=info.get('keywords', ''),
                img_url=info.get('img_url', '')
            )
            
        except Exception as e:
            logger.error(f"아티스트 정보 수집 실패: {e}")
            return None
    
    def _format_date(self, date_str: Optional[str]) -> str:
        """날짜 형식 통일"""
        if not date_str:
            return ''
        
        # YYYY-MM-DD 형식으로 통일
        date_str = date_str.replace('.', '-').replace('/', '-')
        return date_str
    
    def _determine_status(self, start_date: Optional[str], end_date: Optional[str]) -> str:
        """날짜 기반 상태 결정"""
        from datetime import datetime, date
        
        if not start_date:
            return 'UNKNOWN'
        
        try:
            start = datetime.strptime(start_date, '%Y-%m-%d').date()
            today = date.today()
            
            if start > today:
                return 'UPCOMING'
            elif end_date:
                end = datetime.strptime(end_date, '%Y-%m-%d').date()
                if end >= today:
                    return 'ONGOING'
                else:
                    return 'PAST'
            else:
                return 'PAST' if start < today else 'ONGOING'
                
        except ValueError:
            return 'UNKNOWN'
    
    def _collect_ticket_info(self, title: str, artist: str) -> Optional[Dict[str, Any]]:
        """티켓 정보 수집"""
        # API 호출 로직 (간소화)
        try:
            query = f"{title} {artist} 티켓 예매"
            response = self.api.query_json(query)
            time.sleep(6)  # API 호출 후 6초 대기
            
            # 응답 파싱 로직
            if response and 'ticket' in response:
                return {
                    'url': response.get('ticket_url', ''),
                    'site': response.get('ticket_site', '')
                }
            
        except Exception as e:
            logger.warning(f"티켓 정보 수집 실패: {e}")
        
        return None
    
    def _collect_additional_info(self, title: str, artist: str) -> Optional[Dict[str, Any]]:
        """추가 정보 수집"""
        try:
            query = f"{title} {artist} 콘서트 정보"
            response = self.api.query_json(query)
            time.sleep(6)  # API 호출 후 6초 대기
            
            if response:
                return {
                    'introduction': response.get('description', ''),
                    'label': response.get('label', '')
                }
            
        except Exception as e:
            logger.warning(f"추가 정보 수집 실패: {e}")
        
        return None
    
    def _collect_artist_basic_info(self, artist_name: str) -> Optional[Dict[str, Any]]:
        """아티스트 기본 정보 수집"""
        try:
            query = f"{artist_name} 아티스트 정보 데뷔 국적"
            response = self.api.query_json(query)
            time.sleep(6)  # API 호출 후 6초 대기
            
            if response:
                return {
                    'name': response.get('name', artist_name),
                    'debut_date': response.get('debut_year', ''),
                    'nationality': response.get('nationality', ''),
                    'group_type': response.get('type', ''),
                    'introduction': response.get('bio', ''),
                    'social_media': response.get('instagram', ''),
                    'keywords': response.get('genres', ''),
                    'img_url': response.get('image', '')
                }
            
        except Exception as e:
            logger.warning(f"아티스트 정보 수집 실패: {e}")
        
        return None