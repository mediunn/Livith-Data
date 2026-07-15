"""
Discord 알림 전송 모듈

- DiscordNotifier: KOPIS 비교 결과를 일반 텍스트 메시지로 전송
- notify_kopis_done / notify_instagram_done: 포럼 채널 webhook embed 전송
"""
import requests
import logging
from datetime import datetime
from typing import Dict, List, Optional, Set
from collections import defaultdict

logger = logging.getLogger(__name__)

COLOR_SUCCESS = 0x57F287  # green
COLOR_ERROR = 0xED4245    # red


# ---------------------------------------------------------------------------
# KOPIS 비교 알림 (일반 텍스트 메시지)
# ---------------------------------------------------------------------------

class DiscordNotifier:
    """Discord 웹훅으로 알림 전송"""

    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        self.max_message_length = 2000

    def send_message(self, content: str) -> bool:
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
        excluded_counts: Dict = None,
        start_date: str = "",
        end_date: str = ""
    ) -> bool:
        excluded_counts = excluded_counts or {}
        new_codes = kopis_codes - db_codes
        removed_codes = db_codes - kopis_codes

        if not new_codes and not removed_codes:
            logger.info("변경 사항 없음 - Discord 알림 스킵")
            return True

        today = datetime.now().strftime("%Y.%m.%d")
        total_excluded = sum(excluded_counts.values())
        total_kopis = len(kopis_codes) + total_excluded

        messages = []

        header = f"🎵 KOPIS 동기화 알림 ({today})\n"
        header += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        header += f"📆 비교 기간: {start_date} ~ {end_date}\n\n"
        header += "📊 통계\n"
        header += f"- KOPIS 내한 공연: {total_kopis}개\n"
        header += f"- DB 공연: {len(db_codes)}개\n"
        header += f"- 새로 추가: {len(new_codes)}개\n"
        header += f"- 사라진 공연: {len(removed_codes)}개"

        for genre, count in sorted(excluded_counts.items()):
            header += f"\n- {genre} 공연 (제외): {count}개"

        if new_codes:
            monthly_stats = defaultdict(int)
            for code in new_codes:
                details = kopis_concerts.get(code, {})
                concert_start = details.get('start_date', '')
                if concert_start and len(concert_start) >= 7:
                    monthly_stats[concert_start[:7]] += 1
            if monthly_stats:
                header += "\n\n📅 월별 새 공연:"
                for month in sorted(monthly_stats.keys()):
                    header += f"\n- {month}: {monthly_stats[month]}개"

        messages.append(header)

        if new_codes:
            new_msg = "\n━━━━━━━━━━━━━━━━━━━━━━\n"
            new_msg += f"✨ 새로 추가된 공연 ({len(new_codes)}개)\n"
            new_msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
            for idx, code in enumerate(sorted(new_codes), 1):
                details = kopis_concerts.get(code, {})
                concert_info = f"\n{idx}. [{code}] {details.get('title', '제목 없음')}\n"
                concert_info += f"　　{details.get('start_date', 'N/A')} ~ {details.get('end_date', 'N/A')}\n"
                if len(new_msg + concert_info) > self.max_message_length - 100:
                    messages.append(new_msg)
                    new_msg = concert_info
                else:
                    new_msg += concert_info
            messages.append(new_msg)

        if removed_codes:
            removed_msg = "\n━━━━━━━━━━━━━━━━━━━━━━\n"
            removed_msg += f"🗑️ 사라진 공연 ({len(removed_codes)}개)\n"
            removed_msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
            removed_msg += "⚠️ 공연 취소 또는 KOPIS에서 삭제된 공연\n"
            for idx, code in enumerate(sorted(removed_codes), 1):
                details = db_concerts.get(code, {})
                concert_info = f"\n{idx}. [{code}] {details.get('title', '제목 없음')}\n"
                concert_info += f"　　{details.get('start_date', 'N/A')} ~ {details.get('end_date', 'N/A')}\n"
                if len(removed_msg + concert_info) > self.max_message_length - 100:
                    messages.append(removed_msg)
                    removed_msg = concert_info
                else:
                    removed_msg += concert_info
            messages.append(removed_msg)

        success = True
        for msg in messages:
            if not self.send_message(msg.strip()):
                success = False
        return success


# ---------------------------------------------------------------------------
# Instagram / KOPIS 파이프라인 완료 알림 (포럼 채널 embed)
# ---------------------------------------------------------------------------

def _format_list(items: List[str], max_chars: int = 1000) -> str:
    if not items:
        return "없음"
    lines = []
    total = 0
    for item in items:
        line = f"• {item}"
        total += len(line) + 1
        if total > max_chars:
            remaining = len(items) - len(lines)
            lines.append(f"... 외 {remaining}개")
            break
        lines.append(line)
    return "\n".join(lines)


def _post(webhook_url: str, payload: dict, params: dict = None) -> Optional[dict]:
    try:
        resp = requests.post(webhook_url, json=payload, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json() if resp.content else {}
    except Exception as e:
        logger.warning(f"Discord 전송 실패: {e}")
        return None


def _send_with_thread(
    webhook_url: str,
    thread_name: str,
    color: int,
    detail_fields: List[Dict],
):
    """포럼 채널: embed로 스레드 생성"""
    embed = {"color": color, "description": thread_name}
    if detail_fields:
        embed["fields"] = [{"name": f["name"], "value": f["value"], "inline": False} for f in detail_fields]
    _post(
        webhook_url,
        payload={"thread_name": thread_name, "embeds": [embed]},
        params={"wait": "true"},
    )


def notify_kopis_done(
    webhook_url: str,
    db_label: str,
    concerts: int,
    artists: int,
    concert_list: Optional[List[str]] = None,
    artist_list: Optional[List[str]] = None,
):
    if not webhook_url or (concerts == 0 and artists == 0):
        return

    thread_name = f"🎵 KOPIS 업데이트 완료  |  {db_label}  |  콘서트 {concerts}개  |  아티스트 {artists}개"

    detail_fields = []
    if concert_list:
        detail_fields.append({"name": "콘서트 목록", "value": _format_list(concert_list)})
    if artist_list:
        detail_fields.append({"name": "아티스트 목록", "value": _format_list(artist_list)})

    _send_with_thread(webhook_url=webhook_url, thread_name=thread_name,
                      color=COLOR_SUCCESS, detail_fields=detail_fields)


def notify_instagram_done(
    webhook_url: str,
    db_label: str,
    concerts: int,
    new_artists: int,
    review_needed: int,
    concert_list: Optional[List[str]] = None,
    artist_list: Optional[List[str]] = None,
):
    if not webhook_url or (concerts == 0 and new_artists == 0 and review_needed == 0):
        return

    review_suffix = f"  |  리뷰 필요 {review_needed}개" if review_needed else ""
    thread_name = f"📸 Instagram 업데이트 완료  |  {db_label}  |  콘서트 {concerts}개  |  신규 아티스트 {new_artists}개{review_suffix}"

    detail_fields = []
    if concert_list:
        detail_fields.append({"name": "콘서트 목록", "value": _format_list(concert_list)})
    if artist_list:
        detail_fields.append({"name": "신규 아티스트 목록", "value": _format_list(artist_list)})

    _send_with_thread(webhook_url=webhook_url, thread_name=thread_name,
                      color=COLOR_SUCCESS, detail_fields=detail_fields)
