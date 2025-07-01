from dataclasses import dataclass
from typing import Optional, List

@dataclass
class Concert:
    title: str
    type: str
    status: str
    date: str
    img_url: str
    artist: str
    venue: Optional[str] = ""
    venues: Optional[str] = ""
    city: Optional[str] = ""
    country: Optional[str] = ""

@dataclass
class Setlist:
    concert_title: str
    title: str
    type: str
    status: str

@dataclass
class SetlistSong:
    setlist_title: str
    song_title: str
    song_artist: str
    order_index: int
    fanchant: str

@dataclass
class Song:
    title: str
    artist: str
    img_url: str
    lyrics: str
    pronunciation: str
    translation: str
    album: Optional[str] = ""
    release_date: Optional[str] = ""
    genre: Optional[str] = ""

@dataclass
class Culture:
    concert_title: str
    content: str
