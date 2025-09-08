#!/usr/bin/env python3
"""
CSV 파일을 SQL INSERT 문으로 변환하는 스크립트
"""

import os
import sys
import csv
import re

def sanitize_value(value):
    """SQL 인젝션 방지를 위한 값 정제"""
    if value is None or value == '':
        return 'NULL'
    
    # 문자열 이스케이프
    value = str(value).replace("'", "''")
    return f"'{value}'"

def csv_to_sql(csv_path, table_name):
    """CSV를 SQL INSERT 문으로 변환"""
    sql_statements = []
    
    # 테이블 생성/삭제 (옵션)
    sql_statements.append(f"-- {table_name} 테이블 데이터")
    sql_statements.append(f"DELETE FROM {table_name};")
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            # NULL 값과 빈 문자열 처리
            values = []
            columns = []
            
            for col, val in row.items():
                columns.append(f"`{col}`")
                values.append(sanitize_value(val))
            
            # INSERT 문 생성
            insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});"
            sql_statements.append(insert_sql)
    
    return '\n'.join(sql_statements)

def main():
    """메인 실행 함수"""
    
    # 데이터 경로 설정
    if len(sys.argv) > 1 and sys.argv[1] == '--prod':
        data_path = 'output/main_output'
        output_file = 'output/livith_data_prod.sql'
        print("🚀 프로덕션 데이터 SQL 변환")
    else:
        data_path = 'output/test_output'
        output_file = 'output/livith_data_test.sql'
        print("🧪 테스트 데이터 SQL 변환")
    
    # 변환할 테이블 목록
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
    
    all_sql = []
    all_sql.append("-- Livith Data SQL Import")
    all_sql.append("-- Generated from CSV files")
    all_sql.append("SET NAMES utf8mb4;")
    all_sql.append("SET FOREIGN_KEY_CHECKS = 0;")
    all_sql.append("")
    
    success_count = 0
    
    for csv_file, table_name in tables:
        csv_path = os.path.join(data_path, csv_file)
        
        if os.path.exists(csv_path):
            try:
                # CSV 파일 확인
                with open(csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    rows = list(reader)
                    
                    if len(rows) <= 1:  # 헤더만 있는 경우
                        print(f"⚠️  {table_name}: 데이터 없음 (스킵)")
                        continue
                
                # SQL 변환
                sql = csv_to_sql(csv_path, table_name)
                all_sql.append(sql)
                all_sql.append("")
                
                row_count = len(rows) - 1  # 헤더 제외
                print(f"✅ {table_name}: {row_count}개 행 변환 완료")
                success_count += 1
                
            except Exception as e:
                print(f"❌ {table_name} 변환 실패: {e}")
        else:
            print(f"⏭️  {table_name}: 파일 없음")
    
    all_sql.append("SET FOREIGN_KEY_CHECKS = 1;")
    
    # SQL 파일 저장
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(all_sql))
    
    print()
    print("=" * 50)
    print(f"📊 변환 완료: {success_count}개 테이블")
    print(f"📁 SQL 파일: {output_file}")
    print()
    print("MySQL에 import하려면:")
    print(f"mysql -u root -p livith_data < {output_file}")

if __name__ == "__main__":
    main()