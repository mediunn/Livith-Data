"""
/추가 커맨드 - 최종 확정된 정보로 DB에 콘서트 등록
"""
import re
import logging
from datetime import datetime
from typing import Optional, Tuple
from lib.config import Config
from lib.prompts import DataCollectionPrompts
from core.apis.serper_api import SerperAPI

logger = logging.getLogger(__name__)

GENRE_NAME_TO_ID = {'JPOP': 1, 'ROCK_METAL': 2, 'RAP_HIPHOP': 3, 'INDIE': 4, 'POP': 5}


def parse_extraction_embed(embed) -> dict:
    """봇의 추출 결과 임베드에서 최종값과 reason_code를 다시 파싱"""
    result = {}
    for field in embed.fields:
        name = field.name
        value = field.value.split("\n")[0]  # 첫 줄만 (출처/신뢰도 텍스트 제외)
        if value == "❌ 없음":
            value = None
        if name == "공연명":
            result["concert_title"] = value
        elif name == "아티스트명":
            result["artist_name"] = value
        elif name == "공연날짜":
            if value:
                parts = value.split(" ~ ")
                result["start_date"] = parts[0]
                result["end_date"] = parts[1] if len(parts) > 1 else parts[0]
            else:
                result["start_date"] = None
                result["end_date"] = None

    footer_text = embed.footer.text or ""
    match = re.search(r"reason_code:(\w*)", footer_text)
    result["reason_code"] = match.group(1) if match else ""

    return result


def upsert_artist(db, artist_name: str, data_collector) -> Optional[int]:
    """아티스트 upsert. 기존 있으면 id 반환, 없으면 신규 생성"""
    db.cursor.execute("SELECT id FROM artists WHERE artist = %s", (artist_name,))
    row = db.cursor.fetchone()
    if row:
        return row[0]

    info = data_collector._collect_artist_basic_info(artist_name) or {}
    now = datetime.now()
    db.cursor.execute("""
        INSERT INTO artists
            (artist, category, detail, instagram_url, twitter_url, keywords, img_url, debut_date, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        artist_name, info.get('category', ''), info.get('detail', ''),
        info.get('instagram_url', ''), info.get('twitter_url', ''),
        info.get('keywords', ''), info.get('img_url', ''), info.get('debut_date', ''),
        now, now,
    ))
    db.commit()
    return db.cursor.lastrowid


def find_duplicate_concert(db, artist_id: int, start_date: str, end_date: str) -> Optional[int]:
    """artist_id + 날짜범위 겹침으로 기존 콘서트 찾기"""
    db.cursor.execute("""
        SELECT id FROM concerts
        WHERE artist_id = %s AND start_date <= %s AND end_date >= %s
    """, (artist_id, end_date, start_date))
    row = db.cursor.fetchone()
    return row[0] if row else None


def register_concert(db, request_id: int, embed, data_collector, gemini_api) -> dict:
    """
    /추가 메인 로직
    Returns: {"success": bool, "request_result": str, "concert_id": int|None}
    """
    parsed = parse_extraction_embed(embed)
    reason_code = parsed["reason_code"]

    # 실패 케이스: 추출 단계에서 이미 실패 사유가 있으면 바로 반영하고 종료
    reason_to_result = {
        "past": "PAST_CONCERT",
        "not_found": "INSUFFICIENT_INFORMATION",
        "ambiguous": "INSUFFICIENT_INFORMATION",
        "festival_multi_artist": "INSUFFICIENT_INFORMATION",
        "unsupported_genre": "UNSUPPORTED_GENRE",
    }
    if reason_code in reason_to_result:
        request_result = reason_to_result[reason_code]
        db.cursor.execute(
            "UPDATE concert_requests SET request_result = %s, updated_at = NOW(3) WHERE id = %s",
            (request_result, request_id)
        )
        db.commit()
        return {"success": False, "request_result": request_result, "concert_id": None}

    # 성공 케이스: 실제 등록 진행
    concert_title = parsed["concert_title"]
    artist_name = parsed["artist_name"]
    start_date = parsed["start_date"]
    end_date = parsed["end_date"] or start_date

    if not concert_title or not artist_name or not start_date:
        db.cursor.execute(
            "UPDATE concert_requests SET request_result = %s, updated_at = NOW(3) WHERE id = %s",
            ("INSUFFICIENT_INFORMATION", request_id)
        )
        db.commit()
        return {"success": False, "request_result": "INSUFFICIENT_INFORMATION", "concert_id": None}

    artist_id = upsert_artist(db, artist_name, data_collector)

    # 중복 콘서트 확인
    concert_id = find_duplicate_concert(db, artist_id, start_date, end_date)

    if not concert_id:
        introduction = data_collector._collect_short_introduction(concert_title, artist_name)
        code = f"discord_req_{request_id}"
        db.cursor.execute("""
            INSERT INTO concerts
                (code, title, start_date, end_date, status, artist, artist_id, introduction, created_at, updated_at)
            VALUES (%s, %s, %s, %s, 'UPCOMING', %s, %s, %s, NOW(3), NOW(3))
        """, (code, concert_title, start_date, end_date, artist_name, artist_id, introduction))
        concert_id = db.cursor.lastrowid

        # schedule 생성 (일자별)
        s = datetime.strptime(start_date, "%Y-%m-%d").date()
        e = datetime.strptime(end_date, "%Y-%m-%d").date()
        days = (e - s).days + 1
        for i in range(days):
            current = s + __import__("datetime").timedelta(days=i)
            category = f"{i+1}일차 콘서트" if days > 1 else "콘서트"
            db.cursor.execute("""
                INSERT INTO schedule (concert_id, category, scheduled_at, type)
                VALUES (%s, %s, %s, 'CONCERT')
            """, (concert_id, category, f"{current} 00:00:00"))

        # 장르 등록
        genre_list = data_collector.collect_concert_genre(artist_name, concert_title) or []
        for genre in genre_list:
            name = genre.get('name', '')
            genre_id = GENRE_NAME_TO_ID.get(name)
            if genre_id:
                db.cursor.execute(
                    "INSERT IGNORE INTO concert_genres (concert_id, concert_title, genre_id, name) VALUES (%s, %s, %s, %s)",
                    (concert_id, concert_title, genre_id, name)
                )

        # Serper로 티켓 URL 검색 후 업데이트
        serper_result = SerperAPI().search_ticket_url(concert_title)
        if serper_result:
            db.cursor.execute(
                "UPDATE concerts SET ticket_url = %s, ticket_site = %s WHERE id = %s",
                (serper_result['url'], serper_result['site'], concert_id)
            )
            logger.info(f"Serper 티켓 URL 등록: {serper_result['url']} ({serper_result['site']})")

    db.cursor.execute(
        "SELECT 1 FROM schedule WHERE concert_id = %s AND type IN ('PRE_TICKETING', 'GENERAL_TICKETING')",
        (concert_id,)
    )
    if not db.cursor.fetchone():
        _insert_ticketing_schedule(db, concert_id, artist_name, concert_title, start_date, end_date, data_collector)
        

    db.cursor.execute(
        "UPDATE concert_requests SET request_result = 'REGISTERED', concert_id = %s, updated_at = NOW(3) WHERE id = %s",
        (concert_id, request_id)
    )
    
    db.commit()

    return {"success": True, "request_result": "REGISTERED", "concert_id": concert_id}


def _insert_ticketing_schedule(db, concert_id: int, artist_name: str, concert_title: str,
                                 start_date: str, end_date: str, data_collector):
    """공식 소스에서 예매 일정(선예매/일반예매) 검색 후 schedule 테이블에 저장"""
    query = DataCollectionPrompts.get_schedule_info_prompt(artist_name, concert_title, start_date, end_date)
    response = data_collector.api.query_json(query, use_search=True)

    if not response:
        return

    entries = response if isinstance(response, list) else [response]

    TYPE_KEYWORDS = {
        'PRE_TICKETING': ['선예매'],
        'GENERAL_TICKETING': ['일반예매', '일반 예매'],
        'ADD_TICKETING': ['추가예매', '추가 예매'],
    }

    for entry in entries:
        category = entry.get('category', '')
        scheduled_at = entry.get('scheduled_at', '')
        if not category or not scheduled_at:
            continue

        schedule_type = None
        for t, keywords in TYPE_KEYWORDS.items():
            if any(kw in category for kw in keywords):
                schedule_type = t
                break

        if schedule_type is None:
            print(f"예매 카테고리 아님, 스킵: category='{category}'")
            continue

        try:
            db.cursor.execute("""
                INSERT INTO schedule (concert_id, category, scheduled_at, type)
                VALUES (%s, %s, %s, %s)
            """, (concert_id, category, scheduled_at, schedule_type))
        except Exception as e:
            print(f"티켓팅 일정 INSERT 실패: {e}")