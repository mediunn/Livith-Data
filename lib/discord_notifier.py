"""
Discord webhook 알림 유틸리티
포럼 채널 webhook: 요약 embed → 스레드 안에 상세 목록
"""
import logging
import requests
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

COLOR_SUCCESS = 0x57F287  # green
COLOR_ERROR = 0xED4245    # red


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
    """포럼 채널: 목록 embed로 스레드 생성 (첫 메시지 = 목록)"""
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

    _send_with_thread(
        webhook_url=webhook_url,
        thread_name=thread_name,
        color=COLOR_SUCCESS,
        detail_fields=detail_fields,
    )


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

    _send_with_thread(
        webhook_url=webhook_url,
        thread_name=thread_name,
        color=COLOR_SUCCESS,
        detail_fields=detail_fields,
    )
