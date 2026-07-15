"""
Instagram 크롤링 → MySQL DB 저장 파이프라인
"""
import csv
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any

from lib.prompts import DataCollectionPrompts, CONCERT_KEYWORDS

logger = logging.getLogger(__name__)

CRAWL_BASE_DIR = Path("data/instagram_crawling")

TICKET_SITE_MAP = {
    'interpark': 'NOL 티켓',
    'nol': 'NOL 티켓',
    'nol티켓': 'NOL 티켓',
    'yes24': '예스24',
    'melon': '멜론티켓',
    '멜론': '멜론티켓',
    'ticketlink': '티켓링크',
    '티켓링크': '티켓링크',
    'naver': '네이버 예약',
    '네이버': '네이버 예약',
}

GENRE_NAME_TO_ID = {
    'JPOP': 1,
    'ROCK_METAL': 2,
    'RAP_HIPHOP': 3,
    'INDIE': 4,
    'POP': 5,
}


class InstagramPipeline:
    def __init__(self, db, instagram_api, gemini_api, data_collector,
                 max_posts: int = 12, generate_introduction: bool = True):
        self.db = db
        self.instagram_api = instagram_api
        self.gemini = gemini_api
        self.data_collector = data_collector
        self.max_posts = max_posts
        self.generate_introduction = generate_introduction

        self._preview_artists: List[Dict] = []
        self._preview_concerts: List[Dict] = []
        self._preview_schedule: List[Dict] = []
        self._preview_genres: List[Dict] = []
        self._review_rows: List[Dict] = []  # start_date 없는 게시물 (수동 검토용)

    def run(self):
        accounts = self._get_accounts()
        if not accounts:
            logger.warning("crawl_history에 등록된 계정이 없습니다.")
            return

        for account, last_crawled_at in accounts:
            logger.info(f"\n{'='*50}")
            logger.info(f"@{account} 처리 시작 (last_crawled_at: {last_crawled_at})")
            try:
                self._process_account(account, last_crawled_at)
            except Exception as e:
                logger.error(f"@{account} 처리 중 오류: {e}")
                continue

        self._save_preview_csvs()

    def _get_accounts(self) -> List[Tuple[str, Optional[datetime]]]:
        self.db.cursor.execute("SELECT account, last_crawled_at FROM crawl_history")
        return self.db.cursor.fetchall()

    def _process_account(self, account: str, last_crawled_at: Optional[datetime]):
        posts = self.instagram_api.fetch_recent_posts(
            account,
            max_posts=self.max_posts,
            since_datetime=last_crawled_at,
        )

        if not posts:
            logger.info(f"@{account}: 새 게시물 없음")
            self._update_crawl_history(account)
            return

        logger.info(f"@{account}: {len(posts)}개 게시물 수집")

        processed = 0
        for post in posts:
            if not self._keyword_filter(post.caption):
                logger.debug(f"키워드 없음, 스킵: {post.shortcode}")
                continue
            try:
                self._process_post(post)
                processed += 1
            except Exception as e:
                logger.error(f"게시물 처리 실패 ({post.shortcode}): {e}")
                continue

        self._update_crawl_history(account)
        logger.info(f"@{account}: {processed}개 게시물 처리 완료")

    def _keyword_filter(self, caption: str) -> bool:
        if not caption:
            return False
        caption_lower = caption.lower()
        return any(kw in caption_lower for kw in CONCERT_KEYWORDS)

    # -------------------------------------------------------------------------
    # 실제 DB 저장 경로
    # -------------------------------------------------------------------------

    def _is_future_concert(self, parsed: Dict) -> bool:
        """start_date가 오늘 이후인지 확인. start_date 없으면 True(통과)."""
        start_date = parsed.get('start_date')
        d = self._parse_date(start_date)
        if d and d < datetime.now().date():
            logger.info(f"과거 공연 스킵: {start_date}")
            return False
        return True

    def _process_post(self, post):
        parsed = self._parse_with_gemini(post)
        if not parsed or not parsed.get('is_concert_post'):
            return

        if not self._is_future_concert(parsed):
            return

        raw_artist = (parsed.get('artist_name') or '').strip()
        code = f"{post.account}_insta_{post.shortcode}"

        if not parsed.get('start_date'):
            artist_name = self._get_korean_artist_name(raw_artist)
            title = (parsed.get('title') or f"{artist_name} 내한공연").strip()
            self._review_rows.append({
                "account": post.account,
                "post_url": post.post_url,
                "artist": artist_name,
                "title": title,
                "start_date": "",
                "venue": parsed.get('venue') or '',
                "ticket_site": self._normalize_ticket_site(parsed.get('ticket_site', '')),
                "ticket_url": parsed.get('ticket_url') or '',
                "image_url": post.image_url or '',
                "caption_preview": (post.caption or '')[:200],
            })
            logger.info(f"start_date 없음 → review CSV: {post.shortcode} ({title})")
            return

        artist_name = self._get_korean_artist_name(raw_artist)
        if not artist_name:
            logger.warning(f"아티스트명 없음, 스킵: {post.shortcode}")
            return

        artist_id, artist_name = self._upsert_artist(artist_name)
        if not artist_id:
            logger.warning(f"artist_id 없음, 스킵: {artist_name}")
            return

        title = (parsed.get('title') or f"{artist_name} 내한공연").strip()
        introduction = self.data_collector._collect_short_introduction(title, artist_name) if self.generate_introduction else ''

        try:
            concert_id = self._upsert_concert(parsed, post, artist_id, artist_name, title, introduction)
            if concert_id:
                self._insert_schedule(parsed, concert_id, code)
                genres = self._insert_genres(parsed, concert_id, artist_name, title, code)
                self._collect_concert_for_csv(parsed, post, code, artist_name, title, genres)
            self.db.commit()
            logger.info(f"처리 완료: {title} (concert_id={concert_id})")
        except Exception as e:
            self.db.rollback()
            raise

    # -------------------------------------------------------------------------
    # CSV 백업 수집 헬퍼
    # -------------------------------------------------------------------------

    def _collect_concert_for_csv(self, parsed: Dict, post, code: str,
                                  artist_name: str, title: str, genres: List[Dict]):
        start_date = self._fmt_date(parsed.get('start_date')) or ''
        end_date = self._fmt_date(parsed.get('end_date')) or start_date
        concert_time = parsed.get('concert_time') or '00:00'

        self._preview_concerts.append({
            "code": code,
            "title": title,
            "artist": artist_name,
            "start_date": start_date,
            "end_date": end_date,
            "venue": parsed.get('venue') or '',
            "ticket_site": self._normalize_ticket_site(parsed.get('ticket_site', '')),
            "ticket_url": parsed.get('ticket_url') or '',
            "poster": post.image_url or '',
            "status": "UPCOMING",
        })

        s = self._parse_date(parsed.get('start_date'))
        e = self._parse_date(parsed.get('end_date')) or s
        if s:
            try:
                days = (e - s).days + 1
                for i in range(days):
                    current = s + timedelta(days=i)
                    category = f"{i+1}일차 콘서트" if days > 1 else "콘서트"
                    self._preview_schedule.append({
                        "concert_code": code,
                        "category": category,
                        "scheduled_at": f"{current.strftime('%Y-%m-%d')} {concert_time}:00",
                        "type": "CONCERT",
                    })
            except Exception as ex:
                logger.warning(f"공연일 CSV 수집 실패: {ex}")

        pre_date = parsed.get('pre_ticketing_date')
        pre_time = parsed.get('pre_ticketing_time')
        if pre_date and pre_time:
            try:
                pre_dt = datetime.strptime(f"{pre_date} {pre_time}", "%Y%m%d %H:%M")
                self._preview_schedule.append({
                    "concert_code": code,
                    "category": "선예매 오픈",
                    "scheduled_at": pre_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "type": "PRE_TICKETING",
                })
            except ValueError:
                pass

        general_date = parsed.get('general_ticketing_date')
        general_time = parsed.get('general_ticketing_time')
        if general_date and general_time:
            try:
                general_dt = datetime.strptime(f"{general_date} {general_time}", "%Y%m%d %H:%M")
                self._preview_schedule.append({
                    "concert_code": code,
                    "category": "일반예매 오픈",
                    "scheduled_at": general_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "type": "GENERAL_TICKETING",
                })
            except ValueError:
                pass

        for genre in genres:
            self._preview_genres.append({
                "id": '',
                "concert_id": '',
                "concert_title": title,
                "genre_id": genre.get('genre_id', ''),
                "name": genre.get('name', ''),
                "concert_code": code,
            })

    def _save_preview_csvs(self):
        total = len(self._preview_concerts) + len(self._review_rows)
        if total == 0:
            return

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        run_dir = CRAWL_BASE_DIR / timestamp
        db_dir = run_dir / "db"
        review_dir = run_dir / "review"
        db_dir.mkdir(parents=True, exist_ok=True)

        def write_csv(path, rows, fieldnames):
            with open(path, "w", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            print(f"  → {path} ({len(rows)}행)")

        print(f"\n[CSV 백업] 저장 위치: {run_dir}")
        write_csv(db_dir / "artists.csv", self._preview_artists, [
            "artist", "category", "detail", "instagram_url", "twitter_url",
            "keywords", "img_url", "debut_date", "nationality", "group_type",
            "musicbrainz_id", "status",
        ])
        write_csv(db_dir / "concerts.csv", self._preview_concerts, [
            "code", "title", "artist", "start_date", "end_date",
            "venue", "ticket_site", "ticket_url", "poster", "status",
        ])
        write_csv(db_dir / "schedule.csv", self._preview_schedule, [
            "concert_code", "category", "scheduled_at", "type",
        ])
        write_csv(db_dir / "concert_genres.csv", self._preview_genres, [
            "id", "concert_id", "concert_title", "genre_id", "name", "concert_code",
        ])
        if self._review_rows:
            review_dir.mkdir(parents=True, exist_ok=True)
            write_csv(review_dir / "instagram_review.csv", self._review_rows, [
                "account", "post_url", "artist", "title",
                "start_date", "venue", "ticket_site", "ticket_url",
                "image_url", "caption_preview",
            ])
            print(f"  ※ start_date 없는 게시물 {len(self._review_rows)}개 → review/instagram_review.csv")

    def total_preview_rows(self) -> int:
        return len(self._preview_concerts)

    def total_review_rows(self) -> int:
        return len(self._review_rows)

    # -------------------------------------------------------------------------
    # 공통 헬퍼
    # -------------------------------------------------------------------------

    @staticmethod
    def _normalize_ticket_site(raw: str) -> str:
        """Gemini 반환값을 DB 저장 형식으로 정규화"""
        if not raw:
            return ''
        return TICKET_SITE_MAP.get(raw.strip().lower(), raw.strip())

    @staticmethod
    def _fmt_date(yyyymmdd: str) -> Optional[str]:
        """YYYYMMDD → YYYY.MM.DD 변환 (concerts 테이블 형식). 빈 값이면 None 반환."""
        if not yyyymmdd or len(yyyymmdd) != 8:
            return None
        try:
            return datetime.strptime(yyyymmdd, "%Y%m%d").strftime("%Y.%m.%d")
        except ValueError:
            return None

    @staticmethod
    def _parse_date(yyyymmdd: str):
        """YYYYMMDD → date 객체. schedule 계산용."""
        if not yyyymmdd or len(yyyymmdd) != 8:
            return None
        try:
            return datetime.strptime(yyyymmdd, "%Y%m%d").date()
        except ValueError:
            return None

    def _get_korean_artist_name(self, artist_name: str) -> str:
        return self.data_collector._get_korean_artist_name(artist_name)

    def _parse_with_gemini(self, post) -> Optional[Dict]:
        prompt = DataCollectionPrompts.get_instagram_parse_prompt(
            account=post.account,
            caption=post.caption,
            post_url=post.post_url,
        )
        logger.info(f"캡션 앞 200자: {(post.caption or '')[:200]}")
        result = self.gemini.query_json(prompt, use_search=False)
        logger.info(f"Gemini 파싱 결과: {result}")
        time.sleep(2)
        return result

    def _upsert_artist(self, artist_name: str) -> Tuple[Optional[int], str]:
        """(artist_id, 실제_저장된_아티스트명) 반환. 신규면 artist_name 그대로 삽입."""
        self.db.cursor.execute("SELECT id, artist FROM artists WHERE artist = %s", (artist_name,))
        row = self.db.cursor.fetchone()
        if row:
            return row[0], row[1]

        # 대소문자·띄어쓰기 차이 → 한국어명 괄호 기준으로 재조회
        # 스페이스 포함 한국어명도 캡처 (예: "험프 백")
        korean_match = re.search(r'\(([가-힣\s]+)\)', artist_name)
        if korean_match:
            korean_name = korean_match.group(1).strip()
            korean_name_no_space = korean_name.replace(' ', '')

            # 1차: 그대로 매칭
            self.db.cursor.execute(
                "SELECT id, artist FROM artists WHERE artist LIKE %s LIMIT 1",
                (f'%({korean_name})%',)
            )
            row = self.db.cursor.fetchone()
            if row:
                logger.info(f"한국어명 매칭: '{artist_name}' → DB의 '{row[1]}' (id={row[0]})")
                return row[0], row[1]

            # 2차: 공백 제거 후 매칭 (예: "험프 백" → "험프백")
            if korean_name_no_space != korean_name:
                self.db.cursor.execute(
                    "SELECT id, artist FROM artists WHERE REPLACE(artist, ' ', '') LIKE %s LIMIT 1",
                    (f'%({korean_name_no_space})%',)
                )
                row = self.db.cursor.fetchone()
                if row:
                    logger.info(f"한국어명(공백제거) 매칭: '{artist_name}' → DB의 '{row[1]}' (id={row[0]})")
                    return row[0], row[1]

        # 3차: 영문명만 따로 공백·대소문자 무시 매칭
        # (예: "Hump Back" → "humpback" == DB의 "Humpback")
        english_part = artist_name.split('(')[0].strip()
        if english_part:
            english_normalized = english_part.replace(' ', '').lower()
            self.db.cursor.execute(
                "SELECT id, artist FROM artists "
                "WHERE LOWER(REPLACE(TRIM(SUBSTRING_INDEX(artist, '(', 1)), ' ', '')) = %s LIMIT 1",
                (english_normalized,)
            )
            row = self.db.cursor.fetchone()
            if row:
                logger.info(f"영문명(공백제거) 매칭: '{artist_name}' → DB의 '{row[1]}' (id={row[0]})")
                return row[0], row[1]

        logger.info(f"새 아티스트 '{artist_name}' 정보 수집 중...")
        info = self.data_collector._collect_artist_basic_info(artist_name) or {}

        self._preview_artists.append({
            "artist": artist_name,
            "category": info.get('category', ''),
            "detail": info.get('detail', ''),
            "instagram_url": info.get('instagram_url', ''),
            "twitter_url": info.get('twitter_url', ''),
            "keywords": info.get('keywords', ''),
            "img_url": info.get('img_url', ''),
            "debut_date": info.get('debut_date', ''),
            "nationality": info.get('nationality', ''),
            "group_type": info.get('group_type', ''),
            "musicbrainz_id": info.get('musicbrainz_id', ''),
            "status": "신규 INSERT",
        })

        now = datetime.now()
        self.db.cursor.execute("""
            INSERT INTO artists
                (artist, category, detail, instagram_url, twitter_url, keywords, img_url, debut_date, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            artist_name,
            info.get('category', ''),
            info.get('detail', ''),
            info.get('instagram_url', ''),
            info.get('twitter_url', ''),
            info.get('keywords', ''),
            info.get('img_url', ''),
            info.get('debut_date', ''),
            now, now,
        ))
        self.db.commit()
        return self.db.cursor.lastrowid, artist_name

    def _upsert_concert(self, parsed: Dict, post, artist_id: int,
                        artist_name: str, title: str, introduction: str) -> Optional[int]:
        code = f"{post.account}_insta_{post.shortcode}"
        start_date = self._fmt_date(parsed.get('start_date'))
        end_date = self._fmt_date(parsed.get('end_date')) or start_date
        venue = parsed.get('venue')
        ticket_site = self._normalize_ticket_site(parsed.get('ticket_site', ''))
        ticket_url = parsed.get('ticket_url')
        poster = post.image_url or None

        # 같은 게시물 재크롤링 → skip
        self.db.cursor.execute("SELECT id FROM concerts WHERE code = %s", (code,))
        row = self.db.cursor.fetchone()
        if row:
            logger.info(f"이미 처리된 게시물 스킵: {code}")
            return row[0]

        if start_date:
            self.db.cursor.execute(
                "SELECT id, code FROM concerts WHERE artist_id = %s AND start_date = %s",
                (artist_id, start_date)
            )
            existing = self.db.cursor.fetchone()
            if existing:
                concert_id, existing_code = existing
                if '_insta_' in (existing_code or ''):
                    # 인스타 데이터끼리 → UPDATE
                    self.db.cursor.execute("""
                        UPDATE concerts SET
                            title = %s, end_date = %s, venue = %s,
                            poster = %s, ticket_site = %s, ticket_url = %s
                        WHERE id = %s
                    """, (title, end_date, venue, poster, ticket_site, ticket_url, concert_id))
                    logger.info(f"concerts UPDATE (insta→insta): id={concert_id} ({title})")
                else:
                    # KOPIS 데이터 → end_date는 더 늦은 날짜로, NULL인 필드만 채움
                    self.db.cursor.execute("""
                        UPDATE concerts SET
                            end_date    = GREATEST(COALESCE(end_date, %s), COALESCE(%s, end_date)),
                            venue       = COALESCE(NULLIF(venue, ''), %s),
                            poster      = COALESCE(NULLIF(poster, ''), %s),
                            ticket_site = COALESCE(NULLIF(ticket_site, ''), %s),
                            ticket_url  = COALESCE(NULLIF(ticket_url, ''), %s)
                        WHERE id = %s
                    """, (end_date, end_date, venue, poster, ticket_site, ticket_url, concert_id))
                    logger.info(f"KOPIS 데이터 존재, NULL 필드 보완 + end_date 갱신: id={concert_id} ({existing_code})")
                return concert_id

        self.db.cursor.execute("""
            INSERT INTO concerts
                (code, title, artist, artist_id, start_date, end_date, venue,
                 poster, ticket_site, ticket_url, status, introduction)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'UPCOMING', %s)
        """, (code, title, artist_name, artist_id, start_date, end_date,
              venue, poster, ticket_site, ticket_url, introduction))
        concert_id = self.db.cursor.lastrowid
        logger.info(f"concerts INSERT: id={concert_id} ({title})")
        return concert_id

    def _insert_schedule(self, parsed: Dict, concert_id: int, code: str = ''):
        # 공연일
        concert_time = parsed.get('concert_time') or '00:00'
        s = self._parse_date(parsed.get('start_date'))
        e = self._parse_date(parsed.get('end_date')) or s
        if s:
            try:
                days = (e - s).days + 1
                # 기존 CONCERT 타입 schedule 삭제 후 재삽입 (중복/누적 방지)
                self.db.cursor.execute(
                    "DELETE FROM schedule WHERE concert_id = %s AND type = 'CONCERT'",
                    (concert_id,)
                )
                for i in range(days):
                    current = s + timedelta(days=i)
                    category = f"{i+1}일차 콘서트" if days > 1 else "콘서트"
                    scheduled_at = datetime.strptime(
                        f"{current.strftime('%Y-%m-%d')} {concert_time}", "%Y-%m-%d %H:%M"
                    )
                    self.db.cursor.execute("""
                        INSERT INTO schedule (concert_id, category, scheduled_at, type)
                        VALUES (%s, %s, %s, 'CONCERT')
                    """, (concert_id, category, scheduled_at))
            except Exception as e:
                logger.warning(f"공연일 schedule INSERT 실패: {e}")

        # 티켓팅 일정 (시간 있는 경우만)
        pre_date = parsed.get('pre_ticketing_date')
        pre_time = parsed.get('pre_ticketing_time')
        if pre_date and pre_time:
            self._insert_schedule_row(concert_id, pre_date, pre_time, 'PRE_TICKETING', '선예매 오픈')

        general_date = parsed.get('general_ticketing_date')
        general_time = parsed.get('general_ticketing_time')
        if general_date and general_time:
            self._insert_schedule_row(concert_id, general_date, general_time, 'GENERAL_TICKETING', '일반예매 오픈')

    def _insert_schedule_row(self, concert_id: int, date_str: str, time_str: str, type_: str, category: str):
        try:
            scheduled_at = datetime.strptime(f"{date_str} {time_str}", "%Y%m%d %H:%M")
            self.db.cursor.execute("""
                INSERT IGNORE INTO schedule (concert_id, category, scheduled_at, type)
                VALUES (%s, %s, %s, %s)
            """, (concert_id, category, scheduled_at, type_))
            logger.info(f"schedule INSERT: {category} {scheduled_at}")
        except Exception as e:
            logger.warning(f"schedule INSERT 실패: {e}")

    def _insert_genres(self, parsed: Dict, concert_id: int, artist_name: str, title: str,
                       code: str = '') -> List[Dict]:
        collected = []
        try:
            genre_list = self.data_collector.collect_concert_genre(artist_name, title)
            if not genre_list:
                return collected
            for genre in genre_list:
                name = genre.get('name', '')
                genre_id = GENRE_NAME_TO_ID.get(name)
                if genre_id:
                    self.db.cursor.execute(
                        "INSERT IGNORE INTO concert_genres (concert_id, genre_id, concert_title, name) VALUES (%s, %s, %s, %s)",
                        (concert_id, genre_id, title, name)
                    )
                    collected.append({"genre_id": genre_id, "name": name})
        except Exception as e:
            logger.warning(f"concert_genres INSERT 실패: {e}")
        return collected

    def _update_crawl_history(self, account: str):
        now = datetime.now()
        self.db.cursor.execute("""
            INSERT INTO crawl_history (account, last_crawled_at, created_at, updated_at)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE last_crawled_at = VALUES(last_crawled_at), updated_at = VALUES(updated_at)
        """, (account, now, now, now))
        self.db.commit()
