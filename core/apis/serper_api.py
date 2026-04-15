"""
Serper.dev Google Search API 클라이언트
티켓 예매 URL 검색에 사용
"""
import requests
import logging
from typing import Optional
from lib.config import Config

logger = logging.getLogger(__name__)

TICKET_DOMAINS = [
    'nol.yanolja.com',
    'tickets.interpark.com',
    'ticket.melon.com',
    'ticket.yes24.com',
    'ticketlink.co.kr',
]

SITE_NAMES = {
    'nol.yanolja.com': 'NOL 티켓',
    'tickets.interpark.com': 'NOL 티켓',
    'ticket.melon.com': '멜론티켓',
    'ticket.yes24.com': '예스24',
    'ticketlink.co.kr': '티켓링크',
}


class SerperAPI:
    BASE_URL = "https://google.serper.dev/search"

    def __init__(self):
        self.api_key = Config.SERPER_API_KEY

    def search_ticket_url(self, concert_title: str) -> Optional[dict]:
        """공연명으로 티켓 URL 검색"""
        query = f"{concert_title} 티켓"

        try:
            response = requests.post(
                self.BASE_URL,
                headers={
                    'X-API-KEY': self.api_key,
                    'Content-Type': 'application/json',
                },
                json={
                    'q': query,
                    'num': 5,
                    'gl': 'kr',
                    'hl': 'ko',
                },
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            items = data.get('organic', [])
            if not items:
                logger.info(f"Serper 검색 결과 없음: {query}")
                return None

            print(f"  [Serper 검색] 쿼리: {query}")
            for i, item in enumerate(items):
                print(f"  [{i+1}] {item.get('link', '')}")

            for item in items:
                url = item.get('link', '')
                for domain in TICKET_DOMAINS:
                    if domain in url:
                        site = SITE_NAMES.get(domain, domain)
                        logger.info(f"티켓 URL 발견: {url} ({site})")
                        return {'url': url, 'site': site}

            logger.info(f"티켓 사이트 URL 없음: {query}")
            return None

        except Exception as e:
            logger.error(f"Serper 검색 실패: {e}")
            return None
