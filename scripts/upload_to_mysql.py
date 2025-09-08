#!/usr/bin/env python3
"""
MySQL 데이터베이스에 CSV 파일 업로드하는 스크립트
"""

import os
import sys
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

# MySQL 연결 설정
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = os.getenv('DB_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DB_NAME = os.getenv('DB_NAME', 'livith_data')
DB_PORT = int(os.getenv('DB_PORT', 3306))

def create_connection():
    """MySQL 연결 생성"""
    try:
        # SQLAlchemy 엔진 생성
        engine = create_engine(
            f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4'
        )
        return engine
    except Exception as e:
        print(f"❌ 데이터베이스 연결 실패: {e}")
        sys.exit(1)

def upload_csv_to_mysql(csv_path, table_name, engine):
    """CSV 파일을 MySQL 테이블로 업로드"""
    try:
        # CSV 읽기
        df = pd.read_csv(csv_path)
        
        # 빈 데이터프레임 체크
        if df.empty:
            print(f"⚠️  {table_name}: 데이터 없음 (스킵)")
            return False
        
        # MySQL에 업로드 (기존 테이블 있으면 replace)
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',  # 'append'로 바꾸면 추가
            index=False,
            chunksize=1000
        )
        
        print(f"✅ {table_name}: {len(df)}개 행 업로드 완료")
        return True
        
    except Exception as e:
        print(f"❌ {table_name} 업로드 실패: {e}")
        return False

def main():
    """메인 실행 함수"""
    
    # 데이터 경로 설정
    if len(sys.argv) > 1 and sys.argv[1] == '--prod':
        data_path = 'output/main_output'
        print("🚀 프로덕션 데이터 업로드 모드")
    else:
        data_path = 'output/test_output'
        print("🧪 테스트 데이터 업로드 모드")
    
    # 업로드할 테이블 순서 (외래키 제약 고려)
    tables = [
        ('genres.csv', 'genres'),
        ('home_sections.csv', 'home_sections'),
        ('search_sections.csv', 'search_sections'),
        ('concerts.csv', 'concerts'),
        ('artists.csv', 'artists'),
        ('concert_genres.csv', 'concert_genres'),
        ('concert_info.csv', 'concert_info'),
        ('cultures.csv', 'cultures'),
        ('schedule.csv', 'schedule'),
        ('setlists.csv', 'setlists'),
        ('concert_setlists.csv', 'concert_setlists'),
        ('songs.csv', 'songs'),
        ('setlist_songs.csv', 'setlist_songs'),
        ('md.csv', 'md'),
        ('home_concert_sections.csv', 'home_concert_sections'),
    ]
    
    # 데이터베이스 연결
    engine = create_connection()
    print(f"✅ 데이터베이스 연결 성공: {DB_NAME}")
    print()
    
    # 각 CSV 파일 업로드
    success_count = 0
    total_count = 0
    
    for csv_file, table_name in tables:
        csv_path = os.path.join(data_path, csv_file)
        
        if os.path.exists(csv_path):
            total_count += 1
            if upload_csv_to_mysql(csv_path, table_name, engine):
                success_count += 1
        else:
            print(f"⏭️  {table_name}: 파일 없음 ({csv_path})")
    
    print()
    print("=" * 50)
    print(f"📊 업로드 완료: {success_count}/{total_count} 테이블")
    
    # 연결 종료
    engine.dispose()

if __name__ == "__main__":
    main()