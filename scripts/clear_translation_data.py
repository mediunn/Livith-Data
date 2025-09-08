#!/usr/bin/env python3
"""
songs.csv에서 번역과 발음 데이터를 지우는 스크립트 (원본 가사는 보존)
"""
import sys
import os
import csv
import logging
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

def clear_translation_data(csv_path: str):
    """번역/발음 데이터만 지우고 원본 가사는 보존"""
    csv_path = Path(csv_path)
    
    if not csv_path.exists():
        logger.error(f"파일이 존재하지 않습니다: {csv_path}")
        return False
    
    # 백업 생성
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = csv_path.with_suffix(f'.backup_clear_{timestamp}.csv')
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
    
    # 번역/발음 데이터 지우기
    cleared_count = 0
    for song in songs:
        has_translation = song.get('translation', '').strip()
        has_pronunciation = song.get('pronunciation', '').strip()
        
        if has_translation or has_pronunciation:
            song['translation'] = ''
            song['pronunciation'] = ''
            cleared_count += 1
            logger.info(f"데이터 지움: {song.get('title', '')} - {song.get('artist', '')}")
    
    # 필드명 확인
    if songs:
        fieldnames = list(songs[0].keys())
        required_fields = ['lyrics', 'pronunciation', 'translation']
        for field in required_fields:
            if field not in fieldnames:
                fieldnames.append(field)
    else:
        logger.error("곡 데이터가 없습니다.")
        return False
    
    # CSV 저장
    with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        
        for song in songs:
            # 모든 필드 보장
            for field in fieldnames:
                if field not in song:
                    song[field] = ''
            writer.writerow(song)
    
    logger.info(f"✅ 완료: {cleared_count}곡의 번역/발음 데이터를 지웠습니다.")
    logger.info(f"💾 원본 가사는 보존되었습니다.")
    return True

def main():
    if len(sys.argv) != 2:
        print("사용법: python3 scripts/clear_translation_data.py <CSV파일경로>")
        print("예시: python3 scripts/clear_translation_data.py output/main_output/songs.csv")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    
    print("번역/발음 데이터 지우기:")
    print(f"  파일: {csv_path}")
    print("⚠️  원본 가사(lyrics)는 보존됩니다!")
    print("⚠️  백업 파일이 자동 생성됩니다!")
    print("-" * 50)
    
    try:
        success = clear_translation_data(csv_path)
        if success:
            print("✅ 번역/발음 데이터가 성공적으로 지워졌습니다!")
        else:
            print("❌ 처리 중 오류가 발생했습니다.")
            sys.exit(1)
    except Exception as e:
        print(f"❌ 오류: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()