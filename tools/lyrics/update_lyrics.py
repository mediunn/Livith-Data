#!/usr/bin/env python3
"""
Musixmatch API를 사용하여 가사를 업데이트하는 스크립트
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from core.apis.lyrics_updater import LyricsUpdater

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
    
    logger = logging.getLogger(__name__)
    
    try:
        print("="*60)
        print("Musixmatch 가사 업데이트 도구")
        print("="*60)
        
        # 가사 업데이터 초기화
        updater = LyricsUpdater()
        
        # 사용자 입력 받기
        max_songs = input("파일당 최대 처리할 곡 수 (전체: Enter): ").strip()
        max_songs = int(max_songs) if max_songs.isdigit() else None
        
        if max_songs:
            print(f"테스트 모드: 파일당 최대 {max_songs}곡 처리")
        else:
            print("전체 모드: 모든 곡 처리")
        
        print("\n가사 업데이트 시작...")
        
        # 가사 업데이트 실행
        results = updater.update_all_lyrics(max_songs_per_file=max_songs)
        
        # 결과 출력
        print("\n" + "="*60)
        print("가사 업데이트 완료")
        print("="*60)
        print(f"처리된 파일: {results['files_processed']}개")
        print(f"전체 곡 수: {results['total_stats']['total']}곡")
        print(f"업데이트된 곡: {results['total_stats']['updated']}곡")
        print(f"스킵된 곡: {results['total_stats']['skipped']}곡")
        print(f"실패한 곡: {results['total_stats']['failed']}곡")
        print("="*60)
        
        # 파일별 상세 결과 출력 (요청 시)
        if input("\n파일별 상세 결과를 보시겠습니까? (y/N): ").lower() == 'y':
            print("\n파일별 상세 결과:")
            print("-" * 60)
            for file_path, stats in results['file_results'].items():
                print(f"\n{file_path}:")
                print(f"  총 {stats['total']}곡 - 업데이트 {stats['updated']}곡, 스킵 {stats['skipped']}곡, 실패 {stats['failed']}곡")
        
    except ValueError as e:
        logger.error(f"설정 오류: {e}")
        print("\n오류: Musixmatch API 키가 설정되지 않았습니다.")
        print("다음 중 하나의 방법으로 API 키를 설정해주세요:")
        print("1. .env 파일에 MUSIXMATCH_API_KEY=your_api_key 추가")
        print("2. 환경변수로 MUSIXMATCH_API_KEY 설정")
        print("\nMusixmatch API 키 발급: https://developer.musixmatch.com/")
    except KeyboardInterrupt:
        print("\n\n작업이 사용자에 의해 중단되었습니다.")
    except Exception as e:
        logger.error(f"실행 중 오류 발생: {e}")
        print(f"\n오류가 발생했습니다: {e}")
        print("자세한 로그는 lyrics_update.log 파일을 확인해주세요.")


if __name__ == "__main__":
    main()