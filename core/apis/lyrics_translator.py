#!/usr/bin/env python3
"""
가사를 한국어 해석 및 발음으로 변환하는 모듈
원본 가사 손실되지 않도록 처리
"""
import csv
import logging
import time
from pathlib import Path
from typing import List, Dict, Optional
from core.apis.gemini_api import GeminiAPI
from lib.config import Config
from lib.prompts import LyricsPrompts

# CSV 모듈 설정 - 100만 글자 허용
csv.field_size_limit(1000000)

logger = logging.getLogger(__name__)

class LyricsTranslator:
    # 외국어 가사 -> 한국어 번역 + 발음 변환
    # Gemini API를 사용하여 번역/발음 변환
    def __init__(self, output_dir: str = None):
        # output_dir (main_output 디렉토리에 저장)
        Config.validate_api_keys()  # Gemini API 키 확인
        self.gemini_api = GeminiAPI(Config.GEMINI_API_KEY)
        self.output_dir = Path(output_dir or Config.OUTPUT_DIR)
        
    def create_translation_prompt(self, lyrics: str, song_title: str, artist: str) -> str:
        #번역용 프롬프트 생성
        return LyricsPrompts.get_translation_prompt(lyrics, song_title, artist)

    def create_pronunciation_prompt(self, lyrics: str, song_title: str, artist: str) -> str:
        #발음용 프롬프트 생성
        return LyricsPrompts.get_pronunciation_prompt(lyrics, song_title, artist)

    def get_translation(self, lyrics: str, song_title: str, artist: str) -> Optional[str]:
        #가사를 한국어로 번역
        try:
            prompt = self.create_translation_prompt(lyrics, song_title, artist)
            response = self.gemini_api.query(prompt, use_search=False)
            
            if response and response.strip():
                logger.info(f"번역 완료: {song_title} - {artist}")
                return response.strip()
            else:
                logger.warning(f"번역 실패 (응답 없음): {song_title} - {artist}")
                return None
                
        except Exception as e:
            logger.error(f"번역 중 오류: {song_title} - {artist}: {e}")
            return None

    def get_pronunciation(self, lyrics: str, song_title: str, artist: str) -> Optional[str]:
        #가사를 한국어 발음으로 변환
        try:
            prompt = self.create_pronunciation_prompt(lyrics, song_title, artist)
            response = self.gemini_api.query(prompt, use_search=False)
            
            if response and response.strip():
                logger.info(f"발음 변환 완료: {song_title} - {artist}")
                return response.strip()
            else:
                logger.warning(f"발음 변환 실패 (응답 없음): {song_title} - {artist}")
                return None
                
        except Exception as e:
            logger.error(f"발음 변환 중 오류: {song_title} - {artist}: {e}")
            return None

    def read_songs_from_csv(self, csv_path: Path) -> List[Dict[str, str]]:
        #CSV 파일에서 곡 정보 읽기
        songs = []
        
        try:
            # UTF-8-sig 인코딩으로 읽어서 BOM 제거
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # BOM이 남아있을 경우를 위해 키 정리
                    cleaned_row = {}
                    for key, value in row.items():
                        cleaned_key = key.lstrip('\ufeff').strip()  # BOM과 공백 제거
                        cleaned_row[cleaned_key] = value
                    songs.append(cleaned_row)
            
            logger.info(f"{csv_path}에서 {len(songs)}곡 로드")
            
            # 첫 번째 곡의 키 확인 (디버깅용)
            if songs:
                logger.info(f"CSV 필드명: {list(songs[0].keys())}")
            
            return songs
            
        except Exception as e:
            logger.error(f"CSV 읽기 실패 {csv_path}: {e}")
            return []

    def write_songs_to_csv(self, songs: List[Dict[str, str]], csv_path: Path) -> bool:
        #업데이트된 발음, 번역을 CSV 파일에 다시 저장
        try:
            if not songs:
                logger.warning(f"저장할 곡이 없습니다: {csv_path}")
                return False
            
            # 필드명 확인
            fieldnames = list(songs[0].keys())
            required_fields = ['lyrics', 'pronunciation', 'translation']
            for field in required_fields:
                if field not in fieldnames:
                    fieldnames.append(field)
            
            # 새 파일 저장
            with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_NONNUMERIC)
                writer.writeheader()
                
                for song in songs:
                    # 모든 필드가 있도록 보장
                    for field in fieldnames:
                        if field not in song:
                            song[field] = ''
                    writer.writerow(song)
            
            logger.info(f"파일 저장 완료: {csv_path}")
            return True
            
        except Exception as e:
            logger.error(f"CSV 저장 실패 {csv_path}: {e}")
            return False

    def process_lyrics_translation(self, csv_path: str, mode: str = "both", max_songs: int = None) -> Dict[str, int]:
        # 여러 가사 번역/발음 일괄 처리
        # mode: "translation", "pronunciation", "both" 중 선택
        # max_songs: 최대 처리 곡 수
        csv_path = Path(csv_path)
        stats = {
            'total': 0,
            'translation_updated': 0,
            'pronunciation_updated': 0,
            'skipped': 0,
            'failed': 0
        }
        
        # CSV 파일 읽기
        songs = self.read_songs_from_csv(csv_path)
        if not songs:
            logger.error(f"곡 데이터를 읽을 수 없습니다: {csv_path}")
            return stats
        
        # 가사가 있는 곡들만 필터링
        songs_with_lyrics = []
        for song in songs:
            if song.get('lyrics', '').strip():
                songs_with_lyrics.append(song)
        
        if not songs_with_lyrics:
            logger.warning("가사가 있는 곡이 없습니다.")
            return stats
        
        # 처리할 곡 수 제한
        if max_songs:
            songs_with_lyrics = songs_with_lyrics[:max_songs]
        
        stats['total'] = len(songs_with_lyrics)
        
        logger.info(f"가사 {mode} 처리 시작: {len(songs_with_lyrics)}곡")
        print("-" * 60)
        
        for i, song in enumerate(songs_with_lyrics):
            title = song.get('title', '').strip()
            artist = song.get('artist', '').strip()
            lyrics = song.get('lyrics', '').strip()
            current_translation = song.get('translation', '').strip()
            current_pronunciation = song.get('pronunciation', '').strip()
            
            logger.info(f"[{i+1}/{len(songs_with_lyrics)}] 처리 중: {title} - {artist}")
            
            # 번역 처리
            if mode in ["translation", "both"]:
                if current_translation:
                    logger.info(f"이미 번역 있음, 스킵: {title}")
                    stats['skipped'] += 1
                else:
                    logger.info(f"번역 시작: {title}")
                    translation = self.get_translation(lyrics, title, artist)
                    if translation:
                        song['translation'] = translation
                        stats['translation_updated'] += 1
                        
                        # 즉시 저장
                        if self.write_songs_to_csv(songs, csv_path):
                            logger.info(f"💾 번역 저장 완료: {title}")
                        else:
                            logger.error(f"💾 번역 저장 실패: {title}")
                    else:
                        stats['failed'] += 1
                    
                    # API 호출 제한
                    time.sleep(2)
            
            # 발음 처리
            if mode in ["pronunciation", "both"]:
                if current_pronunciation:
                    logger.info(f"이미 발음 있음, 스킵: {title}")
                    stats['skipped'] += 1
                else:
                    logger.info(f"발음 변환 시작: {title}")
                    pronunciation = self.get_pronunciation(lyrics, title, artist)
                    if pronunciation:
                        song['pronunciation'] = pronunciation
                        stats['pronunciation_updated'] += 1
                        
                        # 즉시 저장
                        if self.write_songs_to_csv(songs, csv_path):
                            logger.info(f"💾 발음 저장 완료: {title}")
                        else:
                            logger.error(f"💾 발음 저장 실패: {title}")
                    else:
                        stats['failed'] += 1
                    
                    # API 호출 제한
                    time.sleep(2)
        
        logger.info(f"✅ 처리 완료: {stats}")
        return stats