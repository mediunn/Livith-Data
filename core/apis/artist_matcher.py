"""
아티스트명 매칭 및 검증을 수행하는 모듈
"""
import pandas as pd
import os
import logging
from lib.config import Config
from lib.safe_writer import SafeWriter

logger = logging.getLogger(__name__)

def match_artist_names():
    """
    artists.csv 파일의 artist 필드를 기준으로 
    concerts.csv 파일의 artist 이름을 매칭하여 수정
    """
    try:
        # 파일 경로 설정
        artists_path = os.path.join(Config.OUTPUT_DIR, 'artists.csv')
        concerts_path = os.path.join(Config.OUTPUT_DIR, 'concerts.csv')
        
        # artists.csv 파일 확인
        if not os.path.exists(artists_path):
            logger.warning("artists.csv 파일이 존재하지 않습니다.")
            return
        
        # concerts.csv 파일 확인
        if not os.path.exists(concerts_path):
            logger.warning("concerts.csv 파일이 존재하지 않습니다.")
            return
        
        # CSV 파일 읽기
        artists_df = pd.read_csv(artists_path, encoding='utf-8-sig')
        concerts_df = pd.read_csv(concerts_path, encoding='utf-8-sig')
        
        # artists.csv에서 artist 필드 확인
        if 'artist' not in artists_df.columns:
            logger.error("artists.csv에 'artist' 컬럼이 없습니다.")
            return
        
        # concerts.csv에서 artist 필드 확인
        if 'artist' not in concerts_df.columns:
            logger.error("concerts.csv에 'artist' 컬럼이 없습니다.")
            return
        
        # artist 이름 매핑 딕셔너리 생성
        # artists.csv에서 고유한 artist 이름들 추출
        artist_mapping = {}
        
        for artist_name in artists_df['artist'].unique():
            if pd.notna(artist_name):
                # 소문자 변환하여 대소문자 구분 없이 매칭
                artist_mapping[artist_name.lower().strip()] = artist_name
        
        # concerts.csv의 artist 이름 수정
        updated_count = 0
        for idx, row in concerts_df.iterrows():
            current_artist = row['artist']
            if pd.notna(current_artist):
                # 현재 artist 이름을 소문자로 변환하여 매칭 시도
                current_artist_lower = current_artist.lower().strip()
                
                # 정확히 일치하는 경우
                if current_artist_lower in artist_mapping:
                    new_artist = artist_mapping[current_artist_lower]
                    if new_artist != current_artist:
                        concerts_df.at[idx, 'artist'] = new_artist
                        updated_count += 1
                        logger.info(f"Artist 이름 변경: '{current_artist}' -> '{new_artist}'")
                else:
                    # 부분 일치 검색 (포함 관계)
                    for key, value in artist_mapping.items():
                        if key in current_artist_lower or current_artist_lower in key:
                            concerts_df.at[idx, 'artist'] = value
                            updated_count += 1
                            logger.info(f"Artist 이름 변경: '{current_artist}' -> '{value}'")
                            break
        
        # 수정된 데이터 저장
        if updated_count > 0:
            # 메인 출력 디렉토리인 경우 백업 생성
            if Config.OUTPUT_DIR == Config.MAIN_OUTPUT_DIR and os.path.exists(concerts_path):
                backup_path = SafeWriter._create_backup_if_needed('concerts.csv')
                if backup_path:
                    logger.info(f"📋 백업 생성: {os.path.basename(backup_path)}")
                    
            concerts_df.to_csv(
                concerts_path,
                index=False,
                encoding='utf-8-sig',
                escapechar='\\',
                quoting=1
            )
            logger.info(f"💾 총 {updated_count}개의 artist 이름이 수정되었습니다.")
            print(f"      📝 {updated_count}개의 artist 이름 수정됨")
        else:
            logger.info("수정할 artist 이름이 없습니다.")
            print(f"      ⚪ 수정할 artist 이름 없음")
            
    except Exception as e:
        logger.error(f"Artist 이름 매칭 중 오류 발생: {e}")
        print(f"      ❌ 오류 발생: {str(e)}")