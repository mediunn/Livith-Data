#!/usr/bin/env python3
import pandas as pd
import os

def fix_schedule_field_name():
    """schedule.csv의 필드명을 schedule_at에서 scheduled_at으로 변경"""
    
    schedule_path = '/Users/youz2me/Xcode/Livith-Data/output/schedule.csv'
    
    # 파일 읽기
    df = pd.read_csv(schedule_path)
    
    print("=" * 60)
    print("schedule.csv 필드명 수정")
    print("=" * 60)
    
    # 현재 컬럼 확인
    print(f"현재 컬럼: {df.columns.tolist()}")
    
    # schedule_at을 scheduled_at으로 변경
    if 'schedule_at' in df.columns:
        df = df.rename(columns={'schedule_at': 'scheduled_at'})
        print(f"수정된 컬럼: {df.columns.tolist()}")
        
        # 저장
        df.to_csv(schedule_path, index=False, encoding='utf-8-sig')
        print("\n✅ 필드명 변경 완료: schedule_at → scheduled_at")
    else:
        print("\n이미 scheduled_at 필드를 사용 중입니다.")
    
    print("\n현재 schedule.csv 내용:")
    print(df.to_string(index=False))

if __name__ == "__main__":
    fix_schedule_field_name()