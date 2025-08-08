from dataclasses import dataclass
from typing import Optional, List

@dataclass
class Song:
    title: str
    artist: str  # artist.csv의 artist와 동일한 이름
    lyrics: str = ""  # 다른 API에서 처리
    pronunciation: str = ""  # 다른 API에서 처리
    translation: str = ""  # 다른 API에서 처리
    youtube_id: str = ""  # 유튜브 URL ID (예: fcQ37Ys4wpE)

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
    type: str  # EXPECTED, ONGOING, PAST
    status: str  # 빈 문자열로 설정

@dataclass
class Concert:
    artist: str  # 표시용 아티스트명 (기존 artist_display 내용)
    code: str  # KOPIS 공연 코드 (PF266782, PF269308 등)
    title: str  # 공연 제목
    start_date: str  # 공연 시작 날짜
    end_date: str  # 공연 종료 날짜
    status: str  # 공연 상태 (ONGOING, UPCOMING, PAST)
    poster: str  # 포스터 URL
    sorted_index: int  # 정렬 인덱스
    ticket_site: str  # 티켓 사이트명 (인터파크 티켓, 멜론티켓)
    ticket_url: str  # 티켓 구매 URL
    venue: str  # 공연장

@dataclass
class Culture:
    concert_title: str
    title: str
    content: str

@dataclass
class Schedule:
    concert_title: str
    category: str  # "티켓팅", "09.13(토) 호시노 겐 콘서트" 등
    scheduled_at: str  # YYYY-MM-DD HH:MM:SS 형식 또는 YYYY-MM-DD

@dataclass
class Merchandise:
    concert_title: str
    name: str
    price: str  # nn,nnn원 형식 (예: 35,000원)
    img_url: str

@dataclass
class ConcertInfo:
    concert_title: str
    category: str
    content: str
    img_url: str

@dataclass
class Artist:
    artist: str  # "원어 (한국어)" 형식
    birth_date: int  # 데뷔년도 또는 첫 앨범 출간년도 (정수 형식)
    birth_place: str
    category: str
    detail: str  # 해요체 형식
    instagram_url: str
    keywords: str  # 콤마로 구분
    img_url: str  # 퍼플렉시티로 검색된 대표 사진
