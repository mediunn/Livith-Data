#!/usr/bin/env python3
"""
songs.csv의 가사를 한국어 번역 및 발음으로 변환하는 스크립트
원본 가사는 절대 손실되지 않음
"""
import sys
import os
import logging
from pathlib import Path

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.lyrics_translator import LyricsTranslator

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("사용법:")
        print("  python3 scripts/translate_lyrics.py <CSV파일경로> <모드> [최대처리곡수]")
        print()
        print("모드:")
        print("  translation    - 한국어 번역만")
        print("  pronunciation  - 발음 변환만") 
        print("  both          - 번역 + 발음 변환")
        print()
        print("예시:")
        print("  # 모든 곡을 번역 + 발음 변환")
        print("  python3 scripts/translate_lyrics.py output/main_output/songs.csv both")
        print()
        print("  # 한국어 번역만, 최대 5곡")
        print("  python3 scripts/translate_lyrics.py output/main_output/songs.csv translation 5")
        print()
        print("  # 발음 변환만")
        print("  python3 scripts/translate_lyrics.py output/main_output/songs.csv pronunciation")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    mode = sys.argv[2]
    max_songs = int(sys.argv[3]) if len(sys.argv) == 4 else None
    
    # 모드 검증
    if mode not in ["translation", "pronunciation", "both"]:
        print("❌ 잘못된 모드입니다. translation, pronunciation, both 중 선택하세요.")
        sys.exit(1)
    
    print(f"가사 번역/발음 변환 시작:")
    print(f"  CSV 파일: {csv_path}")
    print(f"  처리 모드: {mode}")
    if max_songs:
        print(f"  최대 처리 곡수: {max_songs}곡")
    else:
        print(f"  최대 처리 곡수: 제한 없음")
    print()
    print("⚠️  중요: 원본 가사는 절대 손실되지 않습니다!")
    print("⚠️  백업 파일이 자동으로 생성됩니다!")
    print("-" * 60)
    
    # LyricsTranslator 초기화
    try:
        translator = LyricsTranslator()
        
        # 번역/발음 변환 실행
        stats = translator.process_lyrics_translation(csv_path, mode, max_songs)
        
        # 결과 출력
        print("\n" + "="*60)
        print("📊 처리 결과:")
        print(f"  처리된 곡 수: {stats['total']}")
        
        if mode in ["translation", "both"]:
            print(f"  번역 완료: {stats['translation_updated']}")
        
        if mode in ["pronunciation", "both"]:
            print(f"  발음 완료: {stats['pronunciation_updated']}")
            
        print(f"  스킵 (이미 있음): {stats['skipped']}")
        print(f"  실패: {stats['failed']}")
        
        total_updated = stats['translation_updated'] + stats['pronunciation_updated']
        
        if total_updated > 0:
            print(f"\n✅ 성공적으로 처리되었습니다!")
            if mode == "translation":
                print(f"   {stats['translation_updated']}곡의 한국어 번역 완료")
            elif mode == "pronunciation":
                print(f"   {stats['pronunciation_updated']}곡의 발음 변환 완료")
            else:
                print(f"   번역: {stats['translation_updated']}곡, 발음: {stats['pronunciation_updated']}곡")
        else:
            print(f"\n⚠️  처리된 곡이 없습니다.")
            if stats['total'] == 0:
                print("가사가 있는 곡이 없습니다.")
            elif stats['skipped'] == stats['total']:
                print("모든 곡이 이미 처리되어 있습니다.")
            elif stats['failed'] > 0:
                print("처리 중 오류가 발생했습니다.")
            
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()