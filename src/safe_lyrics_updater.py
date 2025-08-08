#!/usr/bin/env python3
"""
안전한 가사 업데이터 - 인코딩 문제 방지
"""
import csv
import logging
from pathlib import Path
from typing import List, Dict
import time
import unicodedata
from config import Config
from src.musixmatch_lyrics_api import MusixmatchLyricsAPI

# CSV 모듈 설정 - 큰 필드 허용
csv.field_size_limit(1000000)

logger = logging.getLogger(__name__)

class SafeLyricsUpdater:
    def __init__(self, output_dir: str = None):
        """
        안전한 가사 업데이터 초기화
        """
        Config.validate_musixmatch()
        self.musixmatch_api = MusixmatchLyricsAPI(Config.MUSIXMATCH_API_KEY)
        self.output_dir = Path(output_dir or Config.OUTPUT_DIR)
        
    def clean_text(self, text: str) -> str:
        """텍스트 정리 및 유효성 검사"""
        if not text:
            return ""
        
        # 유니코드 정규화 (NFC)
        text = unicodedata.normalize('NFC', text)
        
        # 제어 문자 제거 (줄바꿈 제외)
        cleaned = ''.join(char for char in text if char == '\n' or not unicodedata.category(char).startswith('C'))
        
        # 유효한 문자만 남기기
        valid_chars = []
        for char in cleaned:
            try:
                # 문자가 인코딩/디코딩 가능한지 확인
                char.encode('utf-8').decode('utf-8')
                valid_chars.append(char)
            except:
                # 인코딩 불가능한 문자는 ? 로 대체
                valid_chars.append('?')
        
        return ''.join(valid_chars)
    
    def validate_lyrics(self, lyrics: str) -> bool:
        """가사 유효성 검사"""
        if not lyrics:
            return False
        
        # 너무 짧은 가사는 무효
        if len(lyrics) < 10:
            return False
        
        # 깨진 문자 패턴 검사
        corrupted_patterns = [
            '뗣겇묆', '령븀쇋', '瓦룔걚', '訝뽫븣',  # 알려진 깨진 패턴
            '\ufffd',  # 유니코드 대체 문자
        ]
        
        for pattern in corrupted_patterns:
            if pattern in lyrics:
                logger.warning(f"깨진 문자 패턴 감지: {pattern[:20]}...")
                return False
        
        # 정상 문자 비율 검사
        normal_chars = 0
        for char in lyrics:
            if char.isalnum() or char.isspace() or char in '.,!?-()[]{}":;\'':
                normal_chars += 1
        
        normal_ratio = normal_chars / len(lyrics)
        if normal_ratio < 0.3:  # 정상 문자가 30% 미만이면 무효
            logger.warning(f"비정상적인 문자 비율: {normal_ratio:.2%}")
            return False
        
        return True
    
    def read_songs_from_csv(self, csv_path: Path) -> List[Dict[str, str]]:
        """CSV 파일에서 곡 정보 안전하게 읽기"""
        songs = []
        
        try:
            # 다양한 인코딩 시도
            encodings = ['utf-8-sig', 'utf-8', 'cp949', 'euc-kr']
            
            for encoding in encodings:
                try:
                    with open(csv_path, 'r', encoding=encoding) as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            # 키와 값 정리
                            cleaned_row = {}
                            for key, value in row.items():
                                if key:
                                    cleaned_key = key.lstrip('\ufeff').strip()
                                    cleaned_value = self.clean_text(value) if value else ""
                                    cleaned_row[cleaned_key] = cleaned_value
                            songs.append(cleaned_row)
                    
                    logger.info(f"{csv_path}에서 {len(songs)}곡 로드 (인코딩: {encoding})")
                    break  # 성공하면 종료
                    
                except UnicodeDecodeError:
                    continue  # 다음 인코딩 시도
            
            if not songs:
                logger.error(f"CSV 파일을 읽을 수 없습니다: {csv_path}")
            
            return songs
            
        except Exception as e:
            logger.error(f"CSV 읽기 실패 {csv_path}: {e}")
            return []
    
    def write_songs_to_csv(self, songs: List[Dict[str, str]], csv_path: Path) -> bool:
        """업데이트된 곡 정보를 안전하게 CSV 파일에 저장"""
        try:
            if not songs:
                logger.warning(f"저장할 곡이 없습니다: {csv_path}")
                return False
            
            # 필드명 확인
            fieldnames = list(songs[0].keys())
            if 'lyrics' not in fieldnames:
                fieldnames.append('lyrics')
            if 'musixmatch_url' not in fieldnames:
                fieldnames.append('musixmatch_url')
            
            # 백업 파일 생성
            backup_path = csv_path.with_suffix('.csv.backup')
            if csv_path.exists():
                import shutil
                shutil.copy2(csv_path, backup_path)
                logger.info(f"기존 파일 백업: {backup_path}")
            
            # 새 파일 저장 (BOM 포함 UTF-8)
            with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()
                
                for song in songs:
                    # 모든 필드 정리
                    cleaned_song = {}
                    for field in fieldnames:
                        value = song.get(field, "")
                        
                        # 가사 필드 특별 처리
                        if field == 'lyrics' and value:
                            # 유효성 검사
                            if not self.validate_lyrics(value):
                                logger.warning(f"가사 유효성 검사 실패: {song.get('title', 'Unknown')}")
                                value = ""  # 무효한 가사는 비움
                            else:
                                # 줄바꿈을 \n으로 통일
                                value = value.replace('\r\n', '\n').replace('\r', '\n')
                                # 줄바꿈 유지 (요청에 따라)
                                # value = value.replace('\n', '  ') # 줄바꿈을 재현하기 위해 주석 처리
                        
                        # 텍스트 정리
                        cleaned_song[field] = self.clean_text(value)
                    
                    writer.writerow(cleaned_song)
            
            logger.info(f"파일 저장 완료: {csv_path} ({len(songs)}곡)")
            return True
            
        except Exception as e:
            logger.error(f"CSV 저장 실패 {csv_path}: {e}")
            # 백업에서 복원
            backup_path = csv_path.with_suffix('.csv.backup')
            if backup_path.exists():
                import shutil
                shutil.copy2(backup_path, csv_path)
                logger.info("백업에서 복원했습니다")
            return False
    
    def update_lyrics_for_file(self, csv_path: Path, max_songs: int = None) -> Dict[str, int]:
        """단일 CSV 파일의 가사 안전하게 업데이트"""
        logger.info(f"가사 업데이트 시작: {csv_path}")
        
        # 곡 정보 로드
        songs = self.read_songs_from_csv(csv_path)
        if not songs:
            return {"total": 0, "updated": 0, "skipped": 0, "failed": 0}
        
        # 최대 곡 수 제한
        if max_songs:
            songs = songs[:max_songs]
            logger.info(f"테스트 모드: 최대 {max_songs}곡만 처리")
        
        stats = {"total": len(songs), "updated": 0, "skipped": 0, "failed": 0}
        
        for i, song in enumerate(songs, 1):
            # 곡 정보 추출
            title = song.get('title', song.get('곡명', 'Unknown'))
            artist = song.get('artist', song.get('아티스트', 'Unknown'))
            existing_lyrics = song.get('lyrics', '').strip()
            
            logger.info(f"처리 중 ({i}/{len(songs)}): '{title}' by {artist}")
            
            # 이미 가사가 있고 유효한 경우 스킵
            if existing_lyrics and self.validate_lyrics(existing_lyrics):
                logger.info("유효한 가사가 이미 있어서 스킵")
                stats["skipped"] += 1
                continue
            
            # Musixmatch에서 가사 검색
            try:
                lyrics_info = self.musixmatch_api.search_and_get_lyrics(title, artist)
                
                if lyrics_info["status"] == "success" and lyrics_info["lyrics"]:
                    # 가사 유효성 검사
                    if self.validate_lyrics(lyrics_info["lyrics"]):
                        song["lyrics"] = self.clean_text(lyrics_info["lyrics"])
                        song["musixmatch_url"] = lyrics_info["musixmatch_url"]
                        stats["updated"] += 1
                        logger.info("가사 업데이트 성공")
                        
                        # 즉시 저장 - 매번 업데이트된 곡을 저장
                        if self.write_songs_to_csv(songs, csv_path):
                            logger.info(f"곡 #{i} 저장 완료: {title}")
                        else:
                            logger.error(f"곡 #{i} 저장 실패: {title}")
                    else:
                        logger.warning("가져온 가사가 유효하지 않음")
                        stats["failed"] += 1
                else:
                    stats["failed"] += 1
                    logger.warning(f"가사를 찾을 수 없음 - 상태: {lyrics_info['status']}")
                
                # API 과부하 방지 (API 제한 체크)
                time.sleep(2.0)
                
                # API 제한 체크 (하루 2000 요청 제한)
                if i % 100 == 0:
                    logger.info(f"100곡 처리 완료. 잠시 대기... ({i}/{len(songs)})")
                    time.sleep(10)  # 추가 대기
                
            except Exception as e:
                logger.error(f"가사 수집 중 오류: {e}")
                stats["failed"] += 1
        
        # 최종 저장 (이미 각각 저장했지만 확인용)
        logger.info(f"최종 저장 확인: {csv_path}")
        if not self.write_songs_to_csv(songs, csv_path):
            logger.error(f"최종 저장 실패: {csv_path}")
        
        return stats


def main():
    """메인 실행 함수"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('safe_lyrics_update.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    try:
        updater = SafeLyricsUpdater()
        
        # songs.csv 파일 찾기
        csv_path = Path(Config.OUTPUT_DIR) / "songs.csv"
        
        if not csv_path.exists():
            print(f"❌ 파일을 찾을 수 없습니다: {csv_path}")
            return
        
        print("=" * 60)
        print("안전한 가사 업데이트 시작")
        print("=" * 60)
        
        # 전체 곡 처리 (API 제한 고려)
        stats = updater.update_lyrics_for_file(csv_path)
        
        print(f"\n결과:")
        print(f"  - 전체: {stats['total']}곡")
        print(f"  - 업데이트: {stats['updated']}곡")
        print(f"  - 스킵: {stats['skipped']}곡")
        print(f"  - 실패: {stats['failed']}곡")
        print("=" * 60)
        
    except Exception as e:
        logger.error(f"실행 중 오류: {e}")
        raise


if __name__ == "__main__":
    main()