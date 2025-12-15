"""
Discord 알림 전송 모듈
"""
import requests
import logging
from datetime import datetime
from typing import Dict, Set
from collections import defaultdict

logger = logging.getLogger(__name__)


class DiscordNotifier:
    """Discord 웹훅으로 알림 전송"""
    
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        self.max_message_length = 2000
    
    def send_message(self, content: str) -> bool:
        """단일 메시지 전송"""
        if not self.webhook_url:
            logger.warning("Discord 웹훅 URL이 설정되지 않았습니다.")
            return False
        
        try:
            response = requests.post(
                self.webhook_url,
                json={"content": content},
                timeout=10
            )
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Discord 알림 전송 실패: {e}")
            return False
    
    def send_compare_result(
        self,
        kopis_codes: Set[str],
        db_codes: Set[str],
        kopis_concerts: Dict,
        db_concerts: Dict,
        jazz_count: int = 0,
        start_date: str = "",
        end_date: str = ""
    ) -> bool:
        """compare 결과를 Discord로 전송"""
        new_codes = kopis_codes - db_codes
        removed_codes = db_codes - kopis_codes
        
        if not new_codes and not removed_codes:
            logger.info("변경 사항 없음 - Discord 알림 스킵")
            return True
        
        today = datetime.now().strftime("%Y.%m.%d")
        total_kopis = len(kopis_codes) + jazz_count
        
        messages = []
        
        # 헤더 + 통계
        header = f"🎵 KOPIS 동기화 알림 ({today})\n"
        header += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        header += f"📆 비교 기간: {start_date} ~ {end_date}\n\n"
        header += "📊 통계\n"
        header += f"- KOPIS 내한 공연: {total_kopis}개\n"
        header += f"- DB 공연: {len(db_codes)}개\n"
        header += f"- 새로 추가: {len(new_codes)}개\n"
        header += f"- 사라진 공연: {len(removed_codes)}개"
        
        if jazz_count > 0:
            header += f"\n- 재즈 공연 (제외): {jazz_count}개"
        
        # 월별 통계 추가
        if new_codes:
            monthly_stats = defaultdict(int)
            for code in new_codes:
                details = kopis_concerts.get(code, {})
                start_date = details.get('start_date', '')
                if start_date and len(start_date) >= 7:
                    month_key = start_date[:7]  # "YYYY.MM"
                    monthly_stats[month_key] += 1
            
            if monthly_stats:
                header += "\n\n📅 월별 새 공연:"
                for month in sorted(monthly_stats.keys()):
                    header += f"\n- {month}: {monthly_stats[month]}개"
        
        messages.append(header)
        
        # 새로 추가된 공연
        if new_codes:
            new_msg = "\n━━━━━━━━━━━━━━━━━━━━━━\n"
            new_msg += f"✨ 새로 추가된 공연 ({len(new_codes)}개)\n"
            new_msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
            
            for idx, code in enumerate(sorted(new_codes), 1):
                details = kopis_concerts.get(code, {})
                concert_info = f"\n{idx}. [{code}] {details.get('title', '제목 없음')}\n"
                concert_info += f"　　{details.get('artist', '아티스트 없음')}\n"
                concert_info += f"　　{details.get('start_date', 'N/A')} ~ {details.get('end_date', 'N/A')}\n"
                
                if len(new_msg + concert_info) > self.max_message_length - 100:
                    messages.append(new_msg)
                    new_msg = concert_info
                else:
                    new_msg += concert_info
            
            messages.append(new_msg)
        
        # 사라진 공연
        if removed_codes:
            removed_msg = "\n━━━━━━━━━━━━━━━━━━━━━━\n"
            removed_msg += f"🗑️ 사라진 공연 ({len(removed_codes)}개)\n"
            removed_msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
            removed_msg += "⚠️ 공연 취소 또는 KOPIS에서 삭제된 공연\n"
            
            for idx, code in enumerate(sorted(removed_codes), 1):
                details = db_concerts.get(code, {})
                concert_info = f"\n{idx}. [{code}] {details.get('title', '제목 없음')}\n"
                concert_info += f"　　{details.get('artist', '아티스트 없음')}\n"
                concert_info += f"　　{details.get('start_date', 'N/A')} ~ {details.get('end_date', 'N/A')}\n"
                
                if len(removed_msg + concert_info) > self.max_message_length - 100:
                    messages.append(removed_msg)
                    removed_msg = concert_info
                else:
                    removed_msg += concert_info
            
            messages.append(removed_msg)
        
        # 메시지 전송
        success = True
        for msg in messages:
            if not self.send_message(msg.strip()):
                success = False
        
        return success