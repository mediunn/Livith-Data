#!/usr/bin/env python3
"""
특정 곡의 가사를 사용자가 제공한 텍스트로 직접 설정하는 스크립트
"""
import sys
import os
import logging

# 프로젝트 루트 경로를 sys.path에 추가
# 스크립트의 위치(tools/lyrics)에서 두 단계 상위로 이동
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from core.apis.lyrics_updater import LyricsUpdater

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    """메인 실행 함수"""
    if len(sys.argv) != 4:
        print("사용법: python tools/lyrics/set_lyrics_manually.py <CSV파일경로> \"<곡 제목>\" \"<가사>\"")
        print()
        print("예시:")
        print('  python tools/lyrics/set_lyrics_manually.py data/main_output/songs.csv "My Song Title" "Line 1\nLine 2"')
        print()
        print("주의: 가사에 줄바꿈이 있는 경우, 쉘에 따라 \n 또는 `n 등으로 처리해야 할 수 있습니다.")
        print('      따옴표(\" \")로 인수를 감싸는 것을 권장합니다.')
        sys.exit(1)

    csv_path = sys.argv[1]
    song_title = sys.argv[2]
    lyrics_text = sys.argv[3]

    print("="*60)
    print("수동 가사 설정 도구")
    print("="*60)
    print(f"CSV 파일: {csv_path}")
    print(f"곡 제목: {song_title}")
    print(f"설정할 가사: \n---\n{lyrics_text}\n---")
    print("="*60)

    try:
        updater = LyricsUpdater()
        success = updater.set_lyrics_for_song(csv_path, song_title, lyrics_text)

        if success:
            print("\n✅ 성공: 가사가 성공적으로 업데이트되었습니다.")
        else:
            print("\n❌ 실패: 가사 업데이트에 실패했습니다. 로그를 확인해주세요.")
            sys.exit(1)

    except Exception as e:
        logging.error(f"스크립트 실행 중 오류 발생: {e}")
        print(f"\n❌ 오류가 발생했습니다: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
