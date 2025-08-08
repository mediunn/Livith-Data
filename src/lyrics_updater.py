#!/usr/bin/env python3
"""
song.csv 파일들의 가사 정보를 Musixmatch API로 업데이트하는 스크립트
"""
import os
import csv
import logging
from pathlib import Path
from typing import List, Dict
import time
from config import Config
from src.musixmatch_lyrics_api import MusixmatchLyricsAPI

# CSV 모듈 설정 - 큰 필드 허용
csv.field_size_limit(1000000)

logger = logging.getLogger(__name__)

class LyricsUpdater:
    def __init__(self, output_dir: str = None):
        """
        가사 업데이터 초기화
        Args:
            output_dir: 출력 디렉토리 (None이면 Config.OUTPUT_DIR 사용)
        """
        # 설정 검증
        Config.validate_musixmatch()
        
        self.musixmatch_api = MusixmatchLyricsAPI(Config.MUSIXMATCH_API_KEY)
        self.output_dir = Path(output_dir or Config.OUTPUT_DIR)
        
    def find_song_csv_files(self) -> List[Path]:
        """output 디렉토리에서 song.csv 파일들을 찾기"""
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
        """CSV 파일에서 곡 정보 읽기"""
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
        """업데이트된 곡 정보를 CSV 파일에 저장"""
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
            
            # 백업 파일 생성
            backup_path = csv_path.with_suffix('.csv.backup')
            if csv_path.exists():
                csv_path.rename(backup_path)
                logger.info(f"기존 파일 백업: {backup_path}")
            
            # 새 파일 저장 - quoting 옵션 추가로 특수문자 처리
            with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()
                
                for song in songs:
                    # lyrics와 musixmatch_url 필드가 없는 경우 빈 문자열 추가
                    if 'lyrics' not in song:
                        song['lyrics'] = ""
                    if 'musixmatch_url' not in song:
                        song['musixmatch_url'] = ""
                    
                    # 가사가 있는 경우 인코딩 확인 및 정리
                    if song.get('lyrics'):
                        lyrics = song['lyrics']
                        # 가사가 제대로 된 문자열인지 확인
                        if isinstance(lyrics, str):
                            # 줄바꿈을 공백 2개로 변경하여 CSV 호환성 개선
                            song['lyrics'] = lyrics.replace('\n', '  ')
                    
                    writer.writerow(song)
            
            logger.info(f"파일 저장 완료: {csv_path} ({len(songs)}곡)")
            return True
            
        except Exception as e:
            logger.error(f"CSV 저장 실패 {csv_path}: {e}")
            return False
    
    def update_lyrics_for_file(self, csv_path: Path, max_songs: int = None) -> Dict[str, int]:
        """단일 CSV 파일의 가사 업데이트"""
        logger.info(f"가사 업데이트 시작: {csv_path}")
        
        # 곡 정보 로드
        songs = self.read_songs_from_csv(csv_path)
        if not songs:
            return {"total": 0, "updated": 0, "skipped": 0, "failed": 0}
        
        # 최대 곡 수 제한 (테스트용)
        if max_songs:
            songs = songs[:max_songs]
            logger.info(f"테스트 모드: 최대 {max_songs}곡만 처리")
        
        stats = {"total": len(songs), "updated": 0, "skipped": 0, "failed": 0}
        
        for i, song in enumerate(songs, 1):
            # 다양한 키 이름 시도
            title = (song.get('title') or
                    song.get('곡명') or
                    song.get('제목') or
                    song.get('song_title') or
                    'Unknown')
            
            artist = (song.get('artist') or
                    song.get('아티스트') or
                    song.get('가수') or
                    song.get('singer') or
                    'Unknown Artist')
            
            existing_lyrics = song.get('lyrics', '').strip()
            
            logger.info(f"처리 중 ({i}/{len(songs)}): '{title}' by {artist}")
            
            # 이미 가사가 있는 경우 스킵
            if existing_lyrics:
                logger.info("가사가 이미 있어서 스킵")
                stats["skipped"] += 1
                continue
            
            # Musixmatch에서 가사 검색
            try:
                lyrics_info = self.musixmatch_api.search_and_get_lyrics(title, artist)
                
                if lyrics_info["status"] == "success" and lyrics_info["lyrics"]:
                    song["lyrics"] = lyrics_info["lyrics"]
                    song["musixmatch_url"] = lyrics_info["musixmatch_url"]
                    stats["updated"] += 1
                    logger.info("가사 업데이트 성공")
                else:
                    stats["failed"] += 1
                    logger.warning(f"가사를 찾을 수 없음 - 상태: {lyrics_info['status']}")
                
                # API 과부하 방지
                time.sleep(1.5)
                
            except Exception as e:
                logger.error(f"가사 수집 중 오류: {e}")
                stats["failed"] += 1
        
        # 업데이트된 곡 정보 저장
        if self.write_songs_to_csv(songs, csv_path):
            logger.info(f"파일 저장 완료: {csv_path}")
        else:
            logger.error(f"파일 저장 실패: {csv_path}")
        
        return stats

    def update_all_lyrics(self, max_songs_per_file: int = None) -> Dict[str, any]:
        """모든 songs.csv 파일의 가사 업데이트"""
        logger.info("전체 가사 업데이트 시작")
        
        song_files = self.find_song_csv_files()
        if not song_files:
            logger.warning("업데이트할 songs.csv 파일이 없습니다")
            return {
                "files_processed": 0, 
                "total_stats": {"total": 0, "updated": 0, "skipped": 0, "failed": 0},
                "file_results": {}
            }
        
        total_stats = {"total": 0, "updated": 0, "skipped": 0, "failed": 0}
        results = {}
        
        for i, csv_file in enumerate(song_files, 1):
            logger.info(f"\n{'='*60}")
            logger.info(f"파일 처리 ({i}/{len(song_files)}): {csv_file}")
            logger.info(f"{'='*60}")
            
            file_stats = self.update_lyrics_for_file(csv_file, max_songs_per_file)
            results[str(csv_file)] = file_stats
            
            # 총계 업데이트
            for key in total_stats:
                total_stats[key] += file_stats[key]
            
            logger.info(f"파일 완료 - 총 {file_stats['total']}곡, "
                       f"업데이트 {file_stats['updated']}곡, "
                       f"스킵 {file_stats['skipped']}곡, "
                       f"실패 {file_stats['failed']}곡")
        
        # 최종 결과
        logger.info(f"\n{'='*60}")
        logger.info("전체 가사 업데이트 완료")
        logger.info(f"처리된 파일 수: {len(song_files)}")
        logger.info(f"전체 곡 수: {total_stats['total']}")
        logger.info(f"업데이트된 곡 수: {total_stats['updated']}")
        logger.info(f"스킵된 곡 수: {total_stats['skipped']}")
        logger.info(f"실패한 곡 수: {total_stats['failed']}")
        logger.info(f"{'='*60}")
        
        return {
            "files_processed": len(song_files),
            "total_stats": total_stats,
            "file_results": results
        }


def main():
    """메인 실행 함수"""
    
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('lyrics_update.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    try:
        # 가사 업데이터 초기화 (Config에서 API 키 자동 로드)
        updater = LyricsUpdater()
        
        # 테스트 모드: 파일당 최대 3곡만 처리 (실제 사용시 None으로 변경)
        results = updater.update_all_lyrics(max_songs_per_file=3)
        
        print("\n" + "="*60)
        print("가사 업데이트 작업 완료")
        print(f"처리된 파일: {results['files_processed']}개")
        print(f"업데이트된 곡: {results['total_stats']['updated']}곡")
        print("="*60)
        
    except ValueError as e:
        logger.error(f"설정 오류: {e}")
        logger.error("MUSIXMATCH_API_KEY를 .env 파일에 설정해주세요.")
        raise
    except Exception as e:
        logger.error(f"메인 실행 중 오류: {e}")
        raise


if __name__ == "__main__":
    main()