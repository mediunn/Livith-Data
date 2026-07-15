"""
Instagram 공개 계정 게시물 수집 API 클라이언트

instaloader로 로그인 후 세션을 재사용합니다.
- web_profile_info 엔드포인트에서 프로필 + 최근 게시물을 한 번에 조회합니다.

세션 파일 위치: data/instagram_session/
"""
import time
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
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
    - web_profile_info 응답에 포함된 최근 게시물을 파싱
    - 공개 계정 대상, 하루 몇 번 체크하는 낮은 빈도 권장
    """

    def __init__(self, username: str, password: str, delay_between_requests: int = 3):
        self.ig_username = username
        self._ig_password = password
        self.delay = delay_between_requests
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

    def _fetch_profile_with_posts(self, username: str) -> Optional[Dict]:
        """web_profile_info로 프로필 + 최근 게시물 한 번에 조회.

        NOTE: 이 엔드포인트는 최근 게시물을 최대 12개까지만 반환합니다.
        max_posts를 12 이상으로 설정해도 추가로 수집되지 않습니다.
        """
        url = "https://i.instagram.com/api/v1/users/web_profile_info/"
        resp = self.session.get(url, params={"username": username})

        if resp.status_code == 200:
            return resp.json().get("data", {}).get("user")

        if resp.status_code == 401:
            logger.warning(f"@{username}: 세션 만료 (401), 재로그인 후 재시도")
            if self._relogin():
                resp = self.session.get(url, params={"username": username})
                if resp.status_code == 200:
                    return resp.json().get("data", {}).get("user")

        elif resp.status_code == 429:
            logger.warning(f"@{username}: Rate limit (429), 60초 대기 후 재시도")
            time.sleep(60)
            resp = self.session.get(url, params={"username": username})
            if resp.status_code == 200:
                return resp.json().get("data", {}).get("user")

        logger.error(f"@{username}: 프로필 조회 실패 ({resp.status_code})")
        return None

    def fetch_recent_posts(self, username: str, max_posts: int = 20,
                           since_datetime: Optional[datetime] = None) -> List[InstagramPost]:
        """
        특정 계정의 최근 게시물 수집

        Args:
            username: 수집할 Instagram 계정 아이디 (@ 없이)
            max_posts: 최대 수집 게시물 수
            since_datetime: 이 시각 이후 게시물만 수집 (crawl_history.last_crawled_at 기준)

        Returns:
            InstagramPost 리스트 (최신순)
        """
        user = self._fetch_profile_with_posts(username)
        if not user:
            return []

        media_count = user.get("edge_owner_to_timeline_media", {}).get("count", "?")
        user_id = user.get("id", "?")
        logger.info(f"@{username} 접근 성공 (게시물 {media_count}개, id={user_id})")

        edges = user.get("edge_owner_to_timeline_media", {}).get("edges", [])
        if not edges:
            logger.info(f"@{username}: 게시물 없음 (응답에 포함된 edge 없음)")
            return []

        # since_datetime을 UTC aware로 변환
        since_utc = None
        if since_datetime:
            if since_datetime.tzinfo is None:
                since_utc = since_datetime.replace(tzinfo=timezone.utc)
            else:
                since_utc = since_datetime

        # 전체 파싱 후 필터링 (API 반환 순서가 뒤섞일 수 있어서 break 대신 continue)
        all_posts = []
        for edge in edges:
            node = edge.get("node", {})
            shortcode = node.get("shortcode", "")
            taken_at = node.get("taken_at_timestamp", 0)
            timestamp = datetime.fromtimestamp(taken_at, tz=timezone.utc)

            if since_utc and timestamp <= since_utc:
                continue

            caption = ""
            caption_edges = node.get("edge_media_to_caption", {}).get("edges", [])
            if caption_edges:
                caption = caption_edges[0].get("node", {}).get("text", "")

            image_url = node.get("display_url", "")

            all_posts.append(InstagramPost(
                shortcode=shortcode,
                caption=caption,
                timestamp=timestamp,
                image_url=image_url,
                post_url=f"https://www.instagram.com/p/{shortcode}/",
                account=username,
            ))

        # 최신순 정렬 후 max_posts개 반환
        all_posts.sort(key=lambda p: p.timestamp, reverse=True)
        posts = all_posts[:max_posts]

        logger.info(f"@{username}: {len(posts)}개 수집 (전체 {len(edges)}개 중 last_crawled_at 이후 {len(all_posts)}개)")
        return posts

    def fetch_post_image_url(self, account: str, shortcode: str) -> Optional[str]:
        """account의 최근 게시물에서 shortcode에 해당하는 display_url 반환.
        원본 크롤링과 동일한 web_profile_info 엔드포인트 사용."""
        user = self._fetch_profile_with_posts(account)
        if not user:
            return None
        edges = user.get("edge_owner_to_timeline_media", {}).get("edges", [])
        for edge in edges:
            node = edge.get("node", {})
            if node.get("shortcode") == shortcode:
                time.sleep(self.delay)
                return node.get("display_url")
        logger.warning(f"@{account}: shortcode '{shortcode}' 최근 게시물에서 찾을 수 없음 (12개 초과 시 미지원)")
        return None
