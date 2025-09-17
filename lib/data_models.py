from dataclasses import dataclass
from typing import Optional, List
from datetime import datetime

# 기본 테이블 모델들
@dataclass
class Artist:
    id: Optional[int] = None
    artist: str = ""  # "원어 (한국어)" 형식
    birth_date: str = ""  # 생년월일 (YYYY-MM-DD 형식)
    debut_date: str = ""  # 데뷔연도 (YYYY 형식)
    nationality: str = ""  # 국적
    group_type: str = ""  # 솔로/그룹 유형 (category)
    introduction: str = ""  # 소개 (detail)
    social_media: str = ""  # 소셜미디어 URL (instagram_url)
    keywords: str = ""  # 키워드 (콤마로 구분)
    img_url: str = ""  # 대표 이미지 URL
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class Concert:
    id: Optional[int] = None
    artist: str = ""  # 아티스트명
    title: str = ""  # 공연 제목
    start_date: str = ""  # 공연 시작일 (YYYY-MM-DD)
    end_date: str = ""  # 공연 종료일 (YYYY-MM-DD)
    status: str = ""  # 공연 상태 (UPCOMING, ONGOING, PAST)
    label: str = ""  # 특별 라벨 (매진임박 등)
    introduction: str = ""  # 공연 소개
    img_url: str = ""  # 포스터 이미지 URL
    code: str = ""  # KOPIS 공연 코드
    ticket_site: str = ""  # 티켓 사이트명
    ticket_url: str = ""  # 티켓 구매 URL
    venue: str = ""  # 공연장
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class Setlist:
    id: Optional[int] = None
    title: str = ""  # 셋리스트 제목
    artist: str = ""  # 아티스트명
    img_url: str = ""  # 이미지 URL
    start_date: str = ""  # 시작일
    end_date: str = ""  # 종료일
    venue: str = ""  # 공연장
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class Song:
    title: str = ""  # 곡 제목
    artist: str = ""  # 아티스트명
    lyrics: str = ""  # 가사
    pronunciation: str = ""  # 발음
    translation: str = ""  # 번역
    musixmatch_url: str = ""  # Musixmatch URL
    youtube_id: str = ""  # 유튜브 ID (선택적)

@dataclass
class SetlistSong:
    id: Optional[int] = None
    setlist_id: int = 0  # setlists 테이블의 id
    song_title: str = ""  # 곡 제목
    order_index: int = 0  # 순서
    fanchant: str = ""  # 응원가/팬챈트
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class ConcertSetlist:
    id: Optional[int] = None
    concert_id: int = 0  # concerts 테이블의 id
    setlist_id: int = 0  # setlists 테이블의 id
    type: str = ""  # EXPECTED, ONGOING, PAST
    status: str = ""  # 상태
    concert_title: str = ""  # 콘서트 제목 (조인용)
    setlist_title: str = ""  # 셋리스트 제목 (조인용)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class Genre:
    id: int = 0
    name: str = ""  # 장르명

@dataclass
class ConcertGenre:
    concert_id: int = 0  # concerts 테이블의 id
    concert_title: str = ""  # 콘서트 제목
    genre_id: int = 0  # genres 테이블의 id
    genre_name: str = ""  # 장르명

@dataclass
class Culture:
    id: Optional[int] = None
    concert_id: int = 0  # concerts 테이블의 id
    title: str = ""  # 문화 정보 제목
    content: str = ""  # 내용
    img_url: str = ""  # 이미지 URL
    artist_name: str = ""  # 아티스트명 (조인용)
    concert_title: str = ""  # 콘서트 제목 (조인용)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class Schedule:
    id: Optional[int] = None
    concert_id: int = 0  # concerts 테이블의 id
    category: str = ""  # 일정 카테고리
    scheduled_at: str = ""  # 일정 시간 (YYYY-MM-DD HH:MM:SS)
    concert_title: str = ""  # 콘서트 제목 (조인용)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class Merchandise:
    id: Optional[int] = None
    concert_id: int = 0  # concerts 테이블의 id
    name: str = ""  # 굿즈명 (item_name)
    price: str = ""  # 가격
    img_url: str = ""  # 이미지 URL
    artist_name: str = ""  # 아티스트명 (조인용)
    concert_title: str = ""  # 콘서트 제목 (조인용)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class ConcertInfo:
    id: Optional[int] = None
    concert_id: int = 0  # concerts 테이블의 id
    category: str = ""  # 정보 카테고리
    content: str = ""  # 내용
    img_url: str = ""  # 이미지 URL
    artist_name: str = ""  # 아티스트명 (조인용)
    concert_title: str = ""  # 콘서트 제목 (조인용)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

# 홈/검색 섹션 관련
@dataclass
class HomeSection:
    id: int = 0
    title: str = ""  # 섹션 제목
    is_artist_section: int = 0  # 아티스트 섹션 여부
    is_date_included: int = 0  # 날짜 포함 여부
    sub_heading: str = ""  # 서브 헤딩
    section_code: str = ""  # 섹션 코드
    endpoint: str = ""  # API 엔드포인트
    order: int = 0  # 순서

@dataclass
class HomeConcertSection:
    id: Optional[int] = None
    home_section_id: int = 0  # home_sections 테이블의 id
    concert_id: int = 0  # concerts 테이블의 id
    section_title: str = ""  # 섹션 제목
    concert_title: str = ""  # 콘서트 제목
    sorted_index: int = 0  # 정렬 순서
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class SearchSection:
    id: int = 0
    title: str = ""  # 섹션 제목
    is_artist_section: int = 0  # 아티스트 섹션 여부
    is_date_included: int = 0  # 날짜 포함 여부
    sub_heading: str = ""  # 서브 헤딩
    section_code: str = ""  # 섹션 코드
    endpoint: str = ""  # API 엔드포인트
    order: int = 0  # 순서

@dataclass
class SearchConcertSection:
    id: Optional[int] = None
    search_section_id: int = 0  # search_sections 테이블의 id
    concert_id: int = 0  # concerts 테이블의 id
    section_title: str = ""  # 섹션 제목
    concert_title: str = ""  # 콘서트 제목
    sorted_index: int = 0  # 정렬 순서
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
