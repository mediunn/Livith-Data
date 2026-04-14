#!/usr/bin/env python3
"""
song.csv 파일들의 가사 정보를 Musixmatch + LRCLIB API로 업데이트하는 스크립트
아티스트명과 곡 제목을 사용하여 검색

update_all_lyrics
  └→ find_song_csv_files          (CSV 파일들 찾기)
  └→ update_lyrics_for_file       (파일 하나씩 처리)
       └→ read_songs_from_csv     (CSV 읽기)
       └→ extract_original_artist_name  (아티스트 정리)
       └→ get_title_search_variants     (제목 변형 생성)
       └→ musixmatch_api.get_lyrics     (가사 검색)
       └→ write_songs_to_csv            (결과 저장)
"""
import csv
import logging
from pathlib import Path
from typing import List, Dict
import time
import re # 정규표현식 — 괄호 제거, 패턴 매칭에 사용
from lib.config import Config
from core.apis.musixmatch_lrclib_lyrics_api import LyricsAPI # 가사 검색 API 클라이언트 (Musixmatch + LRCLIB)

# CSV 모듈 설정 - 큰 필드 허용
csv.field_size_limit(1000000)

logger = logging.getLogger(__name__)

class LyricsUpdater:
    # CSV 파일에서 곡 목록을 읽고, Musixmatch/LRCLIB API로 가사를 검색하여 업데이트
    def __init__(self, output_dir: str = None):
        # 설정 검증(Musixmatch API 키가 설정되어 있는지 확인)
        if not Config.MUSIXMATCH_API_KEY:
            raise ValueError("MUSIXMATCH_API_KEY가 설정되지 않았습니다")
        
        self.musixmatch_api = LyricsAPI(Config.MUSIXMATCH_API_KEY)
        self.output_dir = Path(output_dir or Config.OUTPUT_DIR)
        
    def extract_original_artist_name(self, artist: str) -> str:
        # 아티스트 이름의 괄호 앞 부분만 추출 (원어 부분)
        if '(' in artist:
            return artist.split('(')[0].strip()
        return artist.strip()

    def get_title_search_variants(self, title: str) -> list:
        """
        제목 추출
        괄호가 있는 경우: [괄호 제거 제목, 괄호 안 내용]
        괄호가 없는 경우: [원본 제목]
        """
        # 괄호/대괄호 제거한 제목
        clean_title = re.sub(r'\s*[\(\[\{].*?[\)\]\}]', '', title).strip()
        if clean_title and clean_title != title:
            variants = [clean_title]
            # 괄호 안 내용들 추출
            bracket_contents = re.findall(r'[\(\[\{](.*?)[\)\]\}]', title)
            for content in bracket_contents:
                content = content.strip()
                if content:
                    variants.append(content)
            return variants
        return [title]
        
    def find_song_csv_files(self) -> List[Path]:
        #output 디렉토리에서 song.csv 파일들을 찾기
        song_files = []
        
        if not self.output_dir.exists():
            logger.warning(f"출력 디렉토리가 존재하지 않습니다: {self.output_dir}")
            return song_files
        
        # songs.csv 파일들 찾기
        for file_path in self.output_dir.rglob("songs.csv"):
            song_files.append(file_path)
            logger.info(f"songs.csv 발견: {file_path}")
        
        logger.info(f"총 {len(song_files)}개의 songs.csv 파일 발견")
        return song_files
    
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
        #업데이트된 곡 정보를 CSV 파일에 저장
        try:
            if not songs:
                logger.warning(f"저장할 곡이 없습니다: {csv_path}")
                return False
            
            # 필드명 확인 (lyrics 필드가 있는지)
            fieldnames = list(songs[0].keys())
            if 'lyrics' not in fieldnames:
                fieldnames.append('lyrics')
            if 'musixmatch_url' not in fieldnames:
                fieldnames.append('musixmatch_url')
            
            # 새 파일 저장 - quoting 옵션 추가로 특수문자 처리
            with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
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
    
    def update_lyrics_for_file(self, csv_path: Path, max_songs: int = None) -> Dict[str, int]:
        #특정 CSV 파일의 가사 업데이트
        stats = {
            'total': 0,
            'updated': 0,
            'skipped': 0,
            'failed': 0
        }
        
        # CSV 파일 읽기
        songs = self.read_songs_from_csv(csv_path)
        if not songs:
            logger.warning(f"곡이 없습니다: {csv_path}")
            return stats
        
        stats['total'] = len(songs)
        process_count = 0
        
        for i, song in enumerate(songs):
            # 제한된 수만 처리
            if max_songs and process_count >= max_songs:
                stats['skipped'] += (len(songs) - i)
                logger.info(f"최대 처리 수 도달 ({max_songs}곡)")
                break
            
            title = song.get('title', '').strip()
            artist = song.get('artist', '').strip()
            current_lyrics = song.get('lyrics', '').strip()
            
            # 필수 정보 확인
            if not title or not artist:
                logger.warning(f"제목 또는 아티스트 정보 없음: {song}")
                stats['skipped'] += 1
                continue
            
            # 이미 가사가 있으면 스킵
            if current_lyrics:
                logger.info(f"이미 가사 있음, 스킵: {title} - {artist}")
                stats['skipped'] += 1
                continue
            
            # 원어 아티스트명 추출
            original_artist = self.extract_original_artist_name(artist)

            # 제목 검색 변형 생성 (괄호 없는 버전 먼저, 그 다음 괄호 안 내용)
            title_variants = self.get_title_search_variants(title)
            if len(title_variants) > 1:
                logger.info(f"[{i+1}/{len(songs)}] 가사 검색: {title} - {original_artist} (원본: {artist}) (변형: {title_variants})")
            else:
                logger.info(f"[{i+1}/{len(songs)}] 가사 검색: {title} - {original_artist} (원본: {artist})")

            # API 호출 제한 (요청 간 1.5초 대기)
            if process_count > 0:
                time.sleep(1.5)

            # 가사 검색 - 원어 아티스트명 사용, 제목 변형으로 순차 시도
            try:
                lyrics_info = None
                for search_title in title_variants:
                    if search_title != title:
                        logger.info(f"  변형 제목으로 검색: '{search_title}'")
                        time.sleep(1.0)
                    lyrics_info = self.musixmatch_api.get_lyrics(search_title, original_artist)
                    if lyrics_info and lyrics_info.get('lyrics'):
                        if search_title != title:
                            logger.info(f"  변형 제목으로 가사 발견: '{search_title}'")
                        break

                if lyrics_info and lyrics_info.get('lyrics'):
                    song['lyrics'] = lyrics_info['lyrics']
                    song['musixmatch_url'] = lyrics_info.get('url', '')
                    logger.info(f"✅ 가사 업데이트 성공: {title} - {original_artist}")
                    stats['updated'] += 1

                    # 가사를 찾자마자 바로 CSV 저장
                    if self.write_songs_to_csv(songs, csv_path):
                        logger.info(f"💾 즉시 저장 완료: {title}")
                    else:
                        logger.error(f"💾 즉시 저장 실패: {title}")
                else:
                    logger.warning(f"❌ 가사를 찾을 수 없음: {title} - {original_artist}")
                    stats['failed'] += 1

            except Exception as e:
                logger.error(f"가사 검색 실패: {title} - {original_artist}: {e}")
                stats['failed'] += 1
            
            process_count += 1
        
        # 각 곡마다 즉시 저장하므로 마지막 저장은 불필요
        logger.info(f"✅ {csv_path} 처리 완료: {stats['updated']}곡 업데이트됨")
        
        return stats
    
    def update_all_lyrics(self, max_songs_per_file: int = None) -> Dict:
        """모든 songs.csv 파일의 가사 업데이트"""
        # songs.csv 파일 찾기
        song_files = self.find_song_csv_files()
        
        if not song_files:
            logger.warning("songs.csv 파일을 찾을 수 없습니다.")
            return {
                'files_processed': 0,
                'total_stats': {'total': 0, 'updated': 0, 'skipped': 0, 'failed': 0},
                'file_results': {}
            }
        
        # 전체 통계
        total_stats = {
            'total': 0,
            'updated': 0,
            'skipped': 0,
            'failed': 0
        }
        
        file_results = {}
        
        # 각 파일 처리
        for csv_path in song_files:
            logger.info(f"\n{'='*60}")
            logger.info(f"파일 처리 시작: {csv_path}")
            logger.info(f"{'='*60}")
            
            stats = self.update_lyrics_for_file(csv_path, max_songs_per_file)
            
            # 통계 업데이트
            for key in total_stats:
                total_stats[key] += stats[key]
            
            file_results[str(csv_path)] = stats
            
            logger.info(f"파일 처리 완료: {stats}")
        
        return {
            'files_processed': len(song_files),
            'total_stats': total_stats,
            'file_results': file_results
        }
    
    def update_lyrics_manual(self, csv_path: str, song_title: str, manual_artist: str) -> bool:
        """
        수동으로 아티스트 이름을 입력해서 특정 곡의 가사 업데이트
        Args:
            csv_path: CSV 파일 경로
            song_title: 곡 제목
            manual_artist: 수동 입력 아티스트명 (뮤직매치 검색용)
        """
        csv_path = Path(csv_path)
        
        # CSV 파일 읽기
        songs = self.read_songs_from_csv(csv_path)
        if not songs:
            logger.error(f"곡 데이터를 읽을 수 없습니다: {csv_path}")
            return False
        
        # 해당 곡 찾기
        target_song = None
        for song in songs:
            if song.get('title', '').strip().lower() == song_title.lower():
                target_song = song
                break
        
        if not target_song:
            logger.error(f"곡을 찾을 수 없습니다: {song_title}")
            logger.info("사용 가능한 곡들:")
            for i, song in enumerate(songs[:10]):  # 처음 10개만 표시
                print(f"  {i+1}. {song.get('title', '')} - {song.get('artist', '')}")
            if len(songs) > 10:
                print(f"  ... (총 {len(songs)}곡)")
            return False
        
        original_artist = target_song.get('artist', '')
        logger.info(f"곡 발견: {song_title} - {original_artist}")
        logger.info(f"수동 아티스트명으로 검색: {manual_artist}")
        
        # 가사 검색
        try:
            lyrics_info = self.musixmatch_api.get_lyrics(song_title, manual_artist, skip_artist_clean=True)

            if lyrics_info and lyrics_info.get('lyrics'):
                target_song['lyrics'] = lyrics_info['lyrics']
                target_song['musixmatch_url'] = lyrics_info.get('url', '')
                logger.info(f"✅ 가사 업데이트 성공: {song_title} - {manual_artist}")
                
                # 바로 저장
                if self.write_songs_to_csv(songs, csv_path):
                    logger.info(f"💾 저장 완료: {song_title}")
                    return True
                else:
                    logger.error(f"💾 저장 실패: {song_title}")
                    return False
            else:
                logger.warning(f"❌ 가사를 찾을 수 없음: {song_title} - {manual_artist}")
                return False
                
        except Exception as e:
            logger.error(f"가사 검색 실패: {song_title} - {manual_artist}: {e}")
            return False
    
    def update_lyrics_by_artist(self, csv_path: str, target_artist: str, search_artist: str = None) -> Dict[str, int]:
        """
        특정 아티스트의 모든 곡 가사 업데이트
        Args:
            csv_path: CSV 파일 경로
            target_artist: CSV에 있는 아티스트명
            search_artist: Musixmatch 검색용 아티스트명 (None이면 원어 추출 사용)
        """
        csv_path = Path(csv_path)
        stats = {
            'total': 0,
            'updated': 0,
            'skipped': 0,
            'failed': 0
        }
        
        # CSV 파일 읽기
        songs = self.read_songs_from_csv(csv_path)
        if not songs:
            logger.error(f"곡 데이터를 읽을 수 없습니다: {csv_path}")
            return stats
        
        # 해당 아티스트의 곡들 찾기
        artist_songs = []
        for song in songs:
            if song.get('artist', '').strip() == target_artist:
                artist_songs.append(song)
        
        if not artist_songs:
            logger.error(f"아티스트를 찾을 수 없습니다: {target_artist}")
            logger.info("사용 가능한 아티스트들:")
            unique_artists = list(set(song.get('artist', '') for song in songs))
            for i, artist in enumerate(sorted(unique_artists)[:20]):  # 처음 20개만 표시
                if artist.strip():
                    print(f"  {i+1}. {artist}")
            if len(unique_artists) > 20:
                print(f"  ... (총 {len(unique_artists)}명)")
            return stats
        
        stats['total'] = len(artist_songs)
        
        # 검색용 아티스트명 결정
        skip_artist_clean = False
        if search_artist is None:
            search_artist = self.extract_original_artist_name(target_artist)
            logger.info(f"원어 아티스트명 추출: {target_artist} -> {search_artist}")
        else:
            skip_artist_clean = True
            logger.info(f"수동 입력 아티스트명 사용 (정제 생략): {search_artist}")

        logger.info(f"아티스트 '{target_artist}' 의 {len(artist_songs)}곡 가사 업데이트 시작")
        logger.info(f"뮤직매치 검색용 아티스트명: {search_artist}")
        print("-" * 50)

        for i, song in enumerate(artist_songs):
            title = song.get('title', '').strip()
            current_lyrics = song.get('lyrics', '').strip()

            # 필수 정보 확인
            if not title:
                logger.warning(f"제목 없음, 스킵: {song}")
                stats['skipped'] += 1
                continue

            # 이미 가사가 있으면 스킵
            if current_lyrics:
                logger.info(f"이미 가사 있음, 스킵: {title}")
                stats['skipped'] += 1
                continue

            # 제목 검색 변형 생성 (괄호 없는 버전 먼저, 그 다음 괄호 안 내용)
            title_variants = self.get_title_search_variants(title)
            if len(title_variants) > 1:
                logger.info(f"[{i+1}/{len(artist_songs)}] 가사 검색: {title} - {search_artist} (변형: {title_variants})")
            else:
                logger.info(f"[{i+1}/{len(artist_songs)}] 가사 검색: {title} - {search_artist}")

            # API 호출 제한 (요청 간 1.5초 대기)
            if i > 0:
                time.sleep(1.5)

            # 가사 검색 - 제목 변형으로 순차 시도
            try:
                lyrics_info = None
                for search_title in title_variants:
                    if search_title != title:
                        logger.info(f"  변형 제목으로 검색: '{search_title}'")
                        time.sleep(1.0)
                    lyrics_info = self.musixmatch_api.get_lyrics(search_title, search_artist, skip_artist_clean=skip_artist_clean)
                    if lyrics_info and lyrics_info.get('lyrics'):
                        if search_title != title:
                            logger.info(f"  변형 제목으로 가사 발견: '{search_title}'")
                        break

                if lyrics_info and lyrics_info.get('lyrics'):
                    song['lyrics'] = lyrics_info['lyrics']
                    song['musixmatch_url'] = lyrics_info.get('url', '')
                    logger.info(f"✅ 가사 업데이트 성공: {title}")
                    stats['updated'] += 1

                    # 가사를 찾자마자 바로 CSV 저장
                    if self.write_songs_to_csv(songs, csv_path):
                        logger.info(f"💾 즉시 저장 완료: {title}")
                    else:
                        logger.error(f"💾 즉시 저장 실패: {title}")
                else:
                    logger.warning(f"❌ 가사를 찾을 수 없음: {title}")
                    stats['failed'] += 1

            except Exception as e:
                logger.error(f"가사 검색 실패: {title}: {e}")
                stats['failed'] += 1
        
        logger.info(f"✅ 아티스트 '{target_artist}' 처리 완료: {stats}")
        return stats

    def set_lyrics_for_song(self, csv_path: str, song_title: str, lyrics_text: str) -> bool:
        """
        수동으로 특정 곡의 가사 설정
        Args:
            csv_path: CSV 파일 경로
            song_title: 가사를 설정할 곡의 제목
            lyrics_text: 설정할 가사 내용
        """
        csv_path = Path(csv_path)
        songs = self.read_songs_from_csv(csv_path)
        if not songs:
            logger.error(f"CSV 파일에서 곡을 읽을 수 없습니다: {csv_path}")
            return False

        target_song = None
        for song in songs:
            if song.get('title', '').strip().lower() == song_title.lower():
                target_song = song
                break

        if not target_song:
            logger.error(f"'{song_title}' 곡을 찾을 수 없습니다: {csv_path}")
            return False

        target_song['lyrics'] = lyrics_text
        logger.info(f"'{song_title}' 곡의 가사를 수동으로 설정했습니다.")

        if self.write_songs_to_csv(songs, csv_path):
            logger.info(f"💾 CSV 파일 저장 완료: {csv_path}")
            return True
        else:
            logger.error(f"💾 CSV 파일 저장 실패: {csv_path}")
            return False