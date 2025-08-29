#!/usr/bin/env python3
import pandas as pd
import os

def clean_schedule_csv():
    """schedule.csv에서 schedule_at이 비어있는 데이터 제거"""
    
    schedule_path = '/Users/youz2me/Xcode/Livith-Data/output/schedule.csv'
    
    # 파일 읽기
    df = pd.read_csv(schedule_path)
    
    print("=" * 60)
    print("기존 schedule.csv 상태:")
    print(f"전체 데이터: {len(df)}개")
    
    # schedule_at이 비어있는 데이터 확인
    empty_schedule = df[df['schedule_at'].isna() | (df['schedule_at'] == '')]
    print(f"schedule_at이 비어있는 데이터: {len(empty_schedule)}개")
    
    if len(empty_schedule) > 0:
        print("\n비어있는 데이터:")
        for idx, row in empty_schedule.iterrows():
            print(f"  - {row['concert_title']} / {row['category']}")
    
    # schedule_at이 비어있지 않은 데이터만 필터링
    clean_df = df[df['schedule_at'].notna() & (df['schedule_at'] != '')]
    
    # 정리된 데이터 저장
    clean_df.to_csv(schedule_path, index=False, encoding='utf-8-sig')
    
    print("\n" + "=" * 60)
    print("정리 완료:")
    print(f"남은 데이터: {len(clean_df)}개")
    print(f"제거된 데이터: {len(df) - len(clean_df)}개")
    
    print("\n정리된 schedule.csv 내용:")
    print(clean_df[['concert_title', 'category', 'schedule_at']].to_string(index=False))

if __name__ == "__main__":
    clean_schedule_csv()