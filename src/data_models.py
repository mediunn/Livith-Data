from dataclasses import dataclass
from typing import Optional, List

@dataclass
class Song:
    title: str
    artist: str
    lyrics: str = ""  # 다른 API에서 처리
    pronunciation: str = ""  # 다른 API에서 처리
    translation: str = ""  # 다른 API에서 처리
    youtube_id: str = ""

@dataclass
class SetlistSong:
    setlist_title: str
    song_title: str
    setlist_date: str
    order_index: int
    fanchant: str = ""  # 다른 API에서 처리
    venue: str = ""

@dataclass
class Setlist:
    title: str
    start_date: str
    end_date: str
    img_url: str
    artist: str
    venue: str

@dataclass
class ConcertSetlist:
    concert_title: str
    setlist_title: str
    setlist_date: str
    type: str  # ONGOING, EXPECTED, PAST
    status: str  # 1회차, 2회차, 종료 등

@dataclass
class Concert:
    title: str
    start_date: str
    end_date: str
    artist: str  # 원본 아티스트명 (매칭용)
    artist_display: str = ""  # 표기용 아티스트명
    poster: str = ""
    status: str = ""  # ONGOING, UPCOMING, PAST
    venue: str = ""
    ticket_url: str = ""
    sorted_index: int = 0  # 정렬 순서

@dataclass
class Culture:
    concert_title: str
    title: str
    content: str

@dataclass
class Schedule:
    concert_title: str
    category: str  # MM.DD(요일) 스케줄명 형식
    scheduled_at: str  # YYYY-MM-DD HH:MM:SS

@dataclass
class Merchandise:
    concert_title: str
    name: str
    price: str
    img_url: str

@dataclass
class ConcertInfo:
    concert_title: str
    category: str
    content: str
    img_url: str

@dataclass
class Artist:
    artist: str
    birth_date: str
    birth_place: str
    category: str
    detail: str
    instagram_url: str
    keywords: str  # 콤마로 구분
    img_url: str
