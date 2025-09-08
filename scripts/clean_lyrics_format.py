#!/usr/bin/env python3
"""
songs.csv의 원어 가사 포맷팅 정리 스크립트
- 줄바꿈 정규화
- 빈줄 제거
- 공백 정리
"""
import sys
import os
import csv
import logging
import re
from pathlib import Path
import datetime
import shutil

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def clean_lyrics_text(lyrics: str) -> str:
    """가사 텍스트 정리"""
    if not lyrics or not lyrics.strip():
        return lyrics
    
    # 1. 다양한 줄바꿈 형태를 통일 (\r\n, \r, \n -> \n)
    cleaned = re.sub(r'\r\n|\r|\n', '\n', lyrics)
    
    # 2. 각 줄의 앞뒤 공백 제거
    lines = [line.strip() for line in cleaned.split('\n')]
    
    # 3. 빈줄 제거 (완전히 빈 줄만)
    non_empty_lines = [line for line in lines if line]
    
    # 4. 다시 합치기
    result = '\n'.join(non_empty_lines)
    
    return result

def clean_lyrics_format(csv_path: str) -> bool:
    """가사 포맷 정리"""
    csv_path = Path(csv_path)
    
    if not csv_path.exists():
        logger.error(f"파일이 존재하지 않습니다: {csv_path}")
        return False
    
    # 백업 생성
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = csv_path.with_suffix(f'.backup_format_{timestamp}.csv')
    shutil.copy2(csv_path, backup_path)
    logger.info(f"백업 생성: {backup_path}")
    
    # CSV 읽기
    songs = []
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # BOM 제거
            cleaned_row = {}
            for key, value in row.items():
                cleaned_key = key.lstrip('\ufeff').strip()
                cleaned_row[cleaned_key] = value
            songs.append(cleaned_row)
    
    logger.info(f"총 {len(songs)}곡 로드")
    
    # 가사 정리
    cleaned_count = 0
    for song in songs:
        original_lyrics = song.get('lyrics', '')
        
        if original_lyrics and original_lyrics.strip():
            cleaned_lyrics = clean_lyrics_text(original_lyrics)
            
            # 변경사항이 있는 경우만 업데이트
            if cleaned_lyrics != original_lyrics:
                song['lyrics'] = cleaned_lyrics
                cleaned_count += 1
                title = song.get('title', '')
                artist = song.get('artist', '')
                logger.info(f"가사 정리: {title} - {artist}")
                
                # 변경 내용 로깅 (디버그용)
                original_lines = len(original_lyrics.split('\n'))
                cleaned_lines = len(cleaned_lyrics.split('\n'))
                logger.debug(f"  줄 수: {original_lines} -> {cleaned_lines}")
    
    if cleaned_count == 0:
        logger.info("정리할 가사가 없습니다.")
        return True
    
    # 필드명 확인
    if songs:
        fieldnames = list(songs[0].keys())
    else:
        logger.error("곡 데이터가 없습니다.")
        return False
    
    # CSV 저장 - 줄바꿈 처리를 위해 QUOTE_NONNUMERIC 사용
    with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_NONNUMERIC)
        writer.writeheader()
        
        for song in songs:
            # 모든 필드 보장
            for field in fieldnames:
                if field not in song:
                    song[field] = ''
            writer.writerow(song)
    
    logger.info(f"✅ 완료: {cleaned_count}곡의 가사 포맷을 정리했습니다.")
    return True

def main():
    if len(sys.argv) != 2:
        print("사용법: python3 scripts/clean_lyrics_format.py <CSV파일경로>")
        print("예시: python3 scripts/clean_lyrics_format.py output/main_output/songs.csv")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    
    print("가사 포맷 정리:")
    print(f"  파일: {csv_path}")
    print("🔧 처리 내용:")
    print("  - 줄바꿈 정규화 (\\r\\n, \\r -> \\n)")
    print("  - 각 줄 앞뒤 공백 제거")
    print("  - 빈줄 제거")
    print("⚠️  백업 파일이 자동 생성됩니다!")
    print("-" * 50)
    
    try:
        success = clean_lyrics_format(csv_path)
        if success:
            print("✅ 가사 포맷이 성공적으로 정리되었습니다!")
        else:
            print("❌ 처리 중 오류가 발생했습니다.")
            sys.exit(1)
    except Exception as e:
        print(f"❌ 오류: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()