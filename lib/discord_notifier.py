"""
Discord 알림 전송 모듈
"""
import requests
import logging
from datetime import datetime
from typing import Dict, Set

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
        jazz_count: int = 0
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
        header = f"""🎵 KOPIS 동기화 알림 ({today})
━━━━━━━━━━━━━━━━━━━━━━

📊 통계
- KOPIS 내한 공연: {total_kopis}개
- DB 공연: {len(db_codes)}개
- 새로 추가: {len(new_codes)}개
- 사라진 공연: {len(removed_codes)}개"""
        
        if jazz_count > 0:
            header += f"\n• 🎷 재즈 공연 (제외): {jazz_count}개"
        
        messages.append(header)
        
        # 새로 추가된 공연
        if new_codes:
            new_msg = f"""
━━━━━━━━━━━━━━━━━━━━━━
✨ 새로 추가된 공연 ({len(new_codes)}개)
━━━━━━━━━━━━━━━━━━━━━━
"""
            for idx, code in enumerate(sorted(new_codes), 1):
                details = kopis_concerts.get(code, {})
                concert_info = f"""
{idx}. [{code}] {details.get('title', '제목 없음')}
   {details.get('artist', '아티스트 없음')}
   📅 {details.get('start_date', 'N/A')} ~ {details.get('end_date', 'N/A')}
"""
                if len(new_msg + concert_info) > self.max_message_length - 100:
                    messages.append(new_msg)
                    new_msg = concert_info
                else:
                    new_msg += concert_info
            
            messages.append(new_msg)
        
        # 사라진 공연
        if removed_codes:
            removed_msg = f"""
━━━━━━━━━━━━━━━━━━━━━━
🗑️ 사라진 공연 ({len(removed_codes)}개)
━━━━━━━━━━━━━━━━━━━━━━
⚠️ 공연 취소 또는 KOPIS에서 삭제된 공연
"""
            for idx, code in enumerate(sorted(removed_codes), 1):
                details = db_concerts.get(code, {})
                concert_info = f"""
{idx}. [{code}] {details.get('title', '제목 없음')}
   {details.get('artist', '아티스트 없음')}
   📅 {details.get('start_date', 'N/A')} ~ {details.get('end_date', 'N/A')}
"""
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