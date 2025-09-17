#!/usr/bin/env python3
"""
아티스트별로 모든 곡의 가사를 업데이트하는 스크립트
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
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("사용법:")
        print("  python3 scripts/artist_lyrics_update.py <CSV파일경로> <아티스트명> [검색용아티스트명]")
        print()
        print("예시:")
        print("  # 기존 아티스트명에서 원어 추출하여 검색")
        print("  python3 scripts/artist_lyrics_update.py output/main_output/songs.csv \"Pink Sweat$ (핑크스웨츠)\"")
        print()
        print("  # 수동으로 검색용 아티스트명 지정")
        print("  python3 scripts/artist_lyrics_update.py output/main_output/songs.csv \"Pink Sweat$ (핑크스웨츠)\" \"Pink Sweat$\"")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    target_artist = sys.argv[2]
    search_artist = sys.argv[3] if len(sys.argv) == 4 else None
    
    print(f"아티스트별 가사 업데이트 시작:")
    print(f"  CSV 파일: {csv_path}")
    print(f"  대상 아티스트: {target_artist}")
    if search_artist:
        print(f"  검색용 아티스트명: {search_artist}")
    else:
        print(f"  검색용 아티스트명: 자동 추출 (원어 부분)")
    print("-" * 60)
    
    # LyricsUpdater 초기화
    try:
        updater = LyricsUpdater()
        
        # 아티스트별 가사 업데이트 실행
        stats = updater.update_lyrics_by_artist(csv_path, target_artist, search_artist)
        
        # 결과 출력
        print("\n" + "="*60)
        print("📊 업데이트 결과:")
        print(f"  전체 곡 수: {stats['total']}")
        print(f"  업데이트 성공: {stats['updated']}")
        print(f"  스킵 (이미 가사 있음): {stats['skipped']}")
        print(f"  실패: {stats['failed']}")
        
        if stats['updated'] > 0:
            print(f"\n✅ {stats['updated']}곡의 가사를 성공적으로 업데이트했습니다!")
        else:
            print(f"\n⚠️  업데이트된 곡이 없습니다.")
            if stats['total'] == 0:
                print("아티스트를 찾을 수 없거나 곡이 없습니다.")
            elif stats['skipped'] == stats['total']:
                print("모든 곡이 이미 가사를 가지고 있습니다.")
            elif stats['failed'] == stats['total'] - stats['skipped']:
                print("모든 곡의 가사를 찾을 수 없었습니다.")
            
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()