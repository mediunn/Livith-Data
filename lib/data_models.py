"""
데이터 모델 정의
"""
from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class Artist:
    """아티스트 데이터 모델"""
    id: Optional[int] = None
    artist: str = ""
    debut_date: str = ""
    nationality: str = ""
    group_type: str = ""
    introduction: str = ""
    social_media: str = ""
    keywords: str = ""
    img_url: str = ""
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class Concert:
    """콘서트 데이터 모델"""
    id: Optional[int] = None
    artist: str = ""
    title: str = ""
    start_date: str = ""
    end_date: str = ""
    status: str = ""
    label: str = ""
    introduction: str = ""
    img_url: str = ""
    code: str = ""
    ticket_site: str = ""
    ticket_url: str = ""
    venue: str = ""
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class Song:
    """곡 데이터 모델"""
    id: Optional[int] = None
    concert_id: str = ""
    title: str = ""
    artist: str = ""
    lyrics: str = ""
    lyrics_kr: str = ""
    romanized: str = ""
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class Setlist:
    """세트리스트 데이터 모델"""
    id: Optional[int] = None
    concert_id: str = ""
    song_order: int = 0
    title: str = ""
    artist: str = ""
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None