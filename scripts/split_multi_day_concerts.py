#!/usr/bin/env python3
"""
콘서트 날짜가 2일 이상인 경우 schedule에 일별로 나누어서 추가
"""
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import time

def parse_date(date_str):
    """날짜 문자열 파싱"""
    if pd.isna(date_str) or date_str == '':
        return None
    
    # 날짜 형식 정리
    date_str = str(date_str).strip()
    
    try:
        # YYYY.MM.DD 형식 처리
        if '.' in date_str:
            return datetime.strptime(date_str, '%Y.%m.%d')
        # YYYY-MM-DD 형식 처리  
        elif '-' in date_str:
            return datetime.strptime(date_str, '%Y-%m-%d')
        else:
            return None
    except:
        return None

def get_date_range(start_date, end_date):
    """시작일과 종료일 사이의 모든 날짜 반환"""
    if not start_date or not end_date:
        return []
    
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date)
        current_date += timedelta(days=1)
    
    return dates

def split_multi_day_concerts():
    """멀티데이 콘서트를 schedule에 일별로 분할"""
    csv_path = Path('/Users/youz2me/Xcode/Livith-Data/output/main_output')
    
    print("📁 파일 로드 중...")
    concerts_df = pd.read_csv(csv_path / 'concerts.csv')
    schedule_df = pd.read_csv(csv_path / 'schedule.csv')
    
    print(f"  concerts.csv: {len(concerts_df)}개 콘서트")
    print(f"  schedule.csv: {len(schedule_df)}개 스케줄")
    
    # 멀티데이 콘서트 찾기
    print("\n🔍 멀티데이 콘서트 분석...")
    
    multi_day_concerts = []
    for _, concert in concerts_df.iterrows():
        start_date = parse_date(concert['start_date'])
        end_date = parse_date(concert['end_date'])
        
        if start_date and end_date and start_date != end_date:
            days_diff = (end_date - start_date).days + 1
            multi_day_concerts.append({
                'title': concert['title'],
                'artist': concert['artist'],
                'start_date': start_date,
                'end_date': end_date,
                'days': days_diff,
                'venue': concert.get('venue', ''),
                'original_title': concert['title']
            })
    
    print(f"  📊 멀티데이 콘서트: {len(multi_day_concerts)}개")
    
    if not multi_day_concerts:
        print("✅ 모든 콘서트가 단일 날짜입니다.")
        return
    
    # 멀티데이 콘서트 목록 출력
    print("\n📋 멀티데이 콘서트 목록:")
    for concert in multi_day_concerts:
        print(f"  • {concert['title']} ({concert['artist']}): {concert['days']}일간")
        print(f"    {concert['start_date'].strftime('%Y.%m.%d')} ~ {concert['end_date'].strftime('%Y.%m.%d')}")
    
    # 멀티데이 콘서트 제목들 (기존 스케줄에서 제거할 것들)
    multi_day_titles = set([concert['original_title'] for concert in multi_day_concerts])
    
    # 백업 생성
    backup_path = csv_path / f"schedule_backup_{int(time.time())}.csv"
    schedule_df.to_csv(backup_path, index=False, encoding='utf-8')
    print(f"\n💾 백업 생성: {backup_path}")
    
    # 기존 멀티데이 콘서트 스케줄 제거
    print("\n🗑️  기존 멀티데이 콘서트 스케줄 제거...")
    original_count = len(schedule_df)
    schedule_df = schedule_df[~schedule_df['concert_title'].isin(multi_day_titles)]
    removed_count = original_count - len(schedule_df)
    print(f"  ✅ {removed_count}개 기존 스케줄 제거")
    
    # 새로운 스케줄 생성
    print("\n📅 멀티데이 콘서트 스케줄 생성...")
    
    new_schedules = []
    total_added = 0
    
    for concert in multi_day_concerts:
        dates = get_date_range(concert['start_date'], concert['end_date'])
        
        for i, date in enumerate(dates, 1):
            # 스케줄 시간 설정 (기본 19:00)
            scheduled_datetime = date.replace(hour=19, minute=0, second=0)
            
            new_schedule = {
                'concert_title': concert['original_title'],
                'category': f'{i}일차 콘서트',
                'scheduled_at': scheduled_datetime.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            new_schedules.append(new_schedule)
            total_added += 1
        
        print(f"  ✅ {concert['title']}: {len(dates)}개 일정 추가")
    
    if not new_schedules:
        print("  ❌ 추가할 스케줄이 없습니다.")
        return
    
    # 기존 데이터와 병합
    new_schedule_df = pd.DataFrame(new_schedules)
    combined_df = pd.concat([schedule_df, new_schedule_df], ignore_index=True)
    
    # 저장
    combined_df.to_csv(csv_path / 'schedule.csv', index=False, encoding='utf-8')
    
    print(f"\n✅ 완료!")
    print(f"  처리된 콘서트: {len(multi_day_concerts)}개")
    print(f"  추가된 스케줄: {total_added}개")
    print(f"  총 스케줄: {len(combined_df)}개")
    
    # 추가된 스케줄 확인
    if total_added > 0:
        print(f"\n📋 추가된 스케줄 (처음 5개):")
        for schedule in new_schedules[:5]:
            print(f"  • {schedule['concert_title']} ({schedule['category']}): {schedule['scheduled_at']}")
        
        if total_added > 5:
            print(f"  ... 및 {total_added - 5}개 더")

if __name__ == "__main__":
    print("=" * 60)
    print("📅 멀티데이 콘서트 스케줄 분할")
    print("=" * 60)
    split_multi_day_concerts()
    print("=" * 60)