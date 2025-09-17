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
    fanchant_point: str = ""  # 특별한 응원법 (한글 30자 이하)

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
    ticket_site: str  # 티켓 사이트명 (인터파크 티켓, 멜론티켓)
    ticket_url: str  # 티켓 구매 URL
    venue: str  # 공연장
    label: str  # 이슈나 특별한 부분 (예: "(매진임박) 콘서트", "(데뷔 10주년 기념) 콘서트")
    introduction: str  # 콘서트 한 줄 소개

@dataclass
class Culture:
    concert_title: str
    title: str
    content: str
    img_url: str

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
    debut_date: str  # 데뷔연도 (YYYY 형식, 문자열)
    category: str
    detail: str  # 합니다체 형식
    instagram_url: str
    keywords: str  # 콤마로 구분
    img_url: str  # 퍼플렉시티로 검색된 대표 사진

@dataclass
class Genre:
    id: int
    name: str

@dataclass
class ConcertGenre:
    concert_id: str  # concert_title과 같은 값
    concert_title: str  # concerts.csv의 title과 대응
    genre_id: int  # genres.csv의 id
    name: str  # 장르명 (JPOP, RAP_HIPHOP, ROCK_METAL, ACOUSTIC, CLASSIC_JAZZ, ELECTRONIC)
