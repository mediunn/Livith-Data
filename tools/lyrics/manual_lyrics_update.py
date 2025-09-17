#!/usr/bin/env python3
"""
수동으로 아티스트명을 입력해서 특정 곡의 가사를 업데이트하는 스크립트
"""
import sys
import os
import logging
from pathlib import Path

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.apis.lyrics_updater import LyricsUpdater

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    if len(sys.argv) != 4:
        print("사용법: python3 scripts/manual_lyrics_update.py <CSV파일경로> <곡제목> <아티스트명>")
        print("예시: python3 scripts/manual_lyrics_update.py output/main_output/songs.csv \"I Feel Good\" \"Pink Sweat$\"")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    song_title = sys.argv[2]
    manual_artist = sys.argv[3]
    
    print(f"수동 가사 업데이트 시작:")
    print(f"  CSV 파일: {csv_path}")
    print(f"  곡 제목: {song_title}")
    print(f"  수동 아티스트명: {manual_artist}")
    print("-" * 50)
    
    # LyricsUpdater 초기화
    try:
        updater = LyricsUpdater()
        
        # 수동 가사 업데이트 실행
        success = updater.update_lyrics_manual(csv_path, song_title, manual_artist)
        
        if success:
            print(f"\n✅ 성공적으로 가사를 업데이트했습니다!")
        else:
            print(f"\n❌ 가사 업데이트에 실패했습니다.")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()