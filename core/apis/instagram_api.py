"""
Instagram 공개 계정 게시물 수집 API 클라이언트

instaloader로 로그인 후 세션을 재사용합니다.
- instaloader.Profile 기반으로 프로필 + 게시물을 조회합니다.
  (구 i.instagram.com/api/v1/users/web_profile_info/ 엔드포인트는 Instagram이 폐기함, 2026-07 확인)
- 게시물 URL 하나만으로도(계정명 몰라도) 단일 게시물 조회 가능합니다 (메타태그 파싱 방식, 별도 경로).

세션 파일 위치: data/instagram_session/
"""
import re
import html
import time
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path

import instaloader

logger = logging.getLogger(__name__)

SESSION_DIR = Path(__file__).parent.parent.parent / "data" / "instagram_session"

MOBILE_API_HEADERS = {
    "User-Agent": "Instagram 275.0.0.27.98 Android",
    "x-ig-app-id": "936619743392459",
}


class InstagramPost:
    """Instagram 게시물 데이터"""
    def __init__(self, shortcode: str, caption: str, timestamp: datetime,
                 image_url: str, post_url: str, account: str):
        self.shortcode = shortcode
        self.caption = caption
        self.timestamp = timestamp
        self.image_url = image_url
        self.post_url = post_url
        self.account = account

    def to_dict(self) -> Dict[str, Any]:
        return {
            "shortcode": self.shortcode,
            "caption": self.caption,
            "timestamp": self.timestamp.isoformat(),
            "image_url": self.image_url,
            "post_url": self.post_url,
            "account": self.account,
        }


class InstagramAPI:
    """
    Instagram 계정 로그인 기반 게시물 수집 클라이언트

    - instaloader로 로그인 후 세션 재사용 (최초 1회만 로그인)
    - instaloader.Profile 기반으로 프로필/게시물 조회
    - 공개 계정 대상, 하루 몇 번 체크하는 낮은 빈도 권장
    - fetch_post_by_url()로 게시물 URL 하나만으로 단일 게시물 조회 가능 (계정명 불필요)
    """

    def __init__(self, username: str, password: str, delay_between_requests: int = 3):
        self.ig_username = username
        self._ig_password = password
        self.delay = delay_between_requests
        self.loader = None
        self._web_loader = None
        self.session = self._build_session(username, password)

    def _session_file(self, username: str) -> Path:
        return SESSION_DIR / f"session_{username}"

    def _build_session(self, username: str, password: str):
        """instaloader로 로그인 후 requests.Session 반환"""
        SESSION_DIR.mkdir(parents=True, exist_ok=True)
        session_file = self._session_file(username)

        loader = instaloader.Instaloader(quiet=True)

        if session_file.exists():
            try:
                loader.load_session_from_file(username, str(session_file))
                logger.info(f"Instagram 세션 로드 완료 (@{username})")
            except Exception as e:
                logger.warning(f"세션 로드 실패, 재로그인: {e}")
                loader.login(username, password)
                loader.save_session_to_file(str(session_file))
                logger.info("재로그인 성공, 세션 저장")
        else:
            logger.info(f"Instagram 로그인 중 (@{username})...")
            loader.login(username, password)
            loader.save_session_to_file(str(session_file))
            logger.info("로그인 성공, 세션 저장 완료")

        session = loader.context._session
        session.headers.update(MOBILE_API_HEADERS)
        self.loader = loader
        return session

    def _relogin(self) -> bool:
        """세션 만료 시 기존 세션 파일 삭제 후 재로그인"""
        session_file = self._session_file(self.ig_username)
        if session_file.exists():
            session_file.unlink()
        try:
            self.session = self._build_session(self.ig_username, self._ig_password)
            return True
        except Exception as e:
            logger.error(f"재로그인 실패: {e}")
            return False

    def fetch_recent_posts(self, username: str, max_posts: int = 20,
                           since_datetime: Optional[datetime] = None) -> Tuple[List[InstagramPost], bool]:
        """
        특정 계정의 최근 게시물 수집 (instaloader.Profile 기반)

        Args:
            username: 수집할 Instagram 계정 아이디 (@ 없이)
            max_posts: 최대 수집 게시물 수
            since_datetime: 이 시각 이후 게시물만 수집 (crawl_history.last_crawled_at 기준)

        Returns:
            (InstagramPost 리스트(최신순), rate_limited 여부) 튜플
        """
        since_utc = None
        if since_datetime:
            since_utc = since_datetime if since_datetime.tzinfo else since_datetime.replace(tzinfo=timezone.utc)

        try:
            profile = instaloader.Profile.from_username(self.loader.context, username)
        except instaloader.exceptions.TooManyRequestsException:
            logger.warning(f"@{username}: Rate limit (429), 조회 실패")
            return [], True
        except instaloader.exceptions.ConnectionException as e:
            # instaloader는 429/rate-limit 계열을 ConnectionException으로 감쌀 때가 있음
            if "401" in str(e) or "429" in str(e) or "checkpoint" in str(e).lower():
                logger.warning(f"@{username}: 연결/인증 오류로 조회 실패 - {e}")
                return [], True
            logger.error(f"@{username}: 프로필 조회 실패 - {e}")
            return [], False
        except Exception as e:
            logger.error(f"@{username}: 프로필 조회 실패 - {e}")
            return [], False

        logger.info(f"@{username} 접근 성공 (게시물 {profile.mediacount}개)")

        posts = []
        try:
            for post in profile.get_posts():
                timestamp = post.date_utc.replace(tzinfo=timezone.utc)

                if since_utc and timestamp <= since_utc:
                    continue

                posts.append(InstagramPost(
                    shortcode=post.shortcode,
                    caption=post.caption or "",
                    timestamp=timestamp,
                    image_url=post.url,
                    post_url=f"https://www.instagram.com/p/{post.shortcode}/",
                    account=username,
                ))

                if len(posts) >= max_posts:
                    break

                time.sleep(self.delay)  # 게시물 순회 사이에도 약간의 딜레이

        except instaloader.exceptions.TooManyRequestsException:
            logger.warning(f"@{username}: 게시물 순회 중 Rate limit (429) - 지금까지 수집된 {len(posts)}개만 반환")
            return posts, True
        except Exception as e:
            logger.error(f"@{username}: 게시물 순회 중 오류 - {e}")

        logger.info(f"@{username}: {len(posts)}개 수집 (last_crawled_at 이후)")
        return posts, False

    def fetch_post_image_url(self, account: str, shortcode: str) -> Optional[str]:
        """account의 게시물 중 shortcode에 해당하는 이미지 URL 반환 (instaloader.Profile 기반)"""
        try:
            profile = instaloader.Profile.from_username(self.loader.context, account)
            for post in profile.get_posts():
                if post.shortcode == shortcode:
                    time.sleep(self.delay)
                    return post.url
        except Exception as e:
            logger.error(f"@{account}: 이미지 URL 조회 실패 - {e}")
        logger.warning(f"@{account}: shortcode '{shortcode}' 게시물을 찾을 수 없음")
        return None

    _MONTH_MAP = {
        'January': 1, 'February': 2, 'March': 3, 'April': 4,
        'May': 5, 'June': 6, 'July': 7, 'August': 8,
        'September': 9, 'October': 10, 'November': 11, 'December': 12,
    }

    def fetch_post_by_url(self, url: str) -> Optional['InstagramPost']:
        """Instagram 게시물 URL로 단일 게시물 조회 (웹 메타태그 방식, web_profile_info와 무관한 별도 경로)"""
        match = re.search(r'/p/([^/?]+)', url)
        if not match:
            logger.error(f"URL에서 shortcode 추출 실패: {url}")
            return None
        shortcode = match.group(1)
        try:
            import requests
            page_url = f"https://www.instagram.com/p/{shortcode}/"
            resp = requests.get(
                page_url,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"},
                cookies=self.session.cookies,
                timeout=15,
            )
            if resp.status_code != 200:
                logger.error(f"게시물 조회 실패 (shortcode={shortcode}): HTTP {resp.status_code}")
                return None
            text = resp.text

            def _meta(prop: str) -> str:
                m = re.search(rf'<meta[^>]+property="{prop}"[^>]+content="([^"]+)"', text)
                return html.unescape(m.group(1)) if m else ""

            desc = _meta("og:description")
            if not desc:
                logger.error(f"게시물 조회 실패 (shortcode={shortcode}): og:description 메타태그를 찾을 수 없음")
                return None

            og_url = _meta("og:url")
            image_url = _meta("og:image")

            cap_m = re.search(r':\s*"(.*)', desc, re.DOTALL)
            caption = cap_m.group(1).rstrip('". \n') if cap_m else desc

            acc_m = re.search(r'instagram\.com/([^/]+)/p/', og_url)
            account = acc_m.group(1) if acc_m else ""

            date_m = re.search(r'on ([A-Za-z]+) (\d+), (\d{4}):', desc)
            if date_m:
                month = self._MONTH_MAP.get(date_m.group(1), 1)
                timestamp = datetime(int(date_m.group(3)), month, int(date_m.group(2)), tzinfo=timezone.utc)
            else:
                timestamp = datetime.now(tz=timezone.utc)

            return InstagramPost(
                shortcode=shortcode,
                caption=caption,
                timestamp=timestamp,
                image_url=image_url,
                post_url=url,
                account=account,
            )
        except Exception as e:
            logger.error(f"게시물 조회 실패 (shortcode={shortcode}): {e}")
            return None
        