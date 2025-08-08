#!/usr/bin/env python3
import pandas as pd

def show_schedule_mismatches():
    base_path = '/Users/youz2me/Xcode/Livith-Data/output/'
    
    # concerts.csv 타이틀 가져오기
    concerts_df = pd.read_csv(base_path + 'concerts.csv', encoding='utf-8')
    concerts_titles = set(concerts_df['title'].tolist())
    print("=" * 60)
    print("📊 concerts.csv 타이틀 분석")
    print("=" * 60)
    print(f"concerts.csv의 총 타이틀 개수: {len(concerts_titles)}")
    
    # schedule.csv 타이틀 가져오기
    schedule_df = pd.read_csv(base_path + 'schedule.csv', encoding='utf-8')
    
    # concert_title 컬럼 확인
    print("\n" + "=" * 60)
    print("📋 schedule.csv 구조 확인")
    print("=" * 60)
    print(f"schedule.csv 컬럼: {schedule_df.columns.tolist()}")
    print(f"schedule.csv 총 행 개수: {len(schedule_df)}")
    
    # concert_title 컬럼이 있는지 확인
    if 'concert_title' in schedule_df.columns:
        schedule_titles = set(schedule_df['concert_title'].tolist())
    else:
        # 첫 번째 컬럼 사용
        schedule_titles = set(schedule_df.iloc[:, 0].tolist())
    
    print(f"schedule.csv의 고유 타이틀 개수: {len(schedule_titles)}")
    
    # 매칭 분석
    matched = schedule_titles & concerts_titles
    only_in_schedule = schedule_titles - concerts_titles
    only_in_concerts = concerts_titles - schedule_titles
    
    print("\n" + "=" * 60)
    print("🔍 매칭 분석 결과")
    print("=" * 60)
    print(f"✅ 매칭된 타이틀: {len(matched)}개")
    print(f"❌ schedule.csv에만 있는 타이틀: {len(only_in_schedule)}개")
    print(f"❌ concerts.csv에만 있는 타이틀: {len(only_in_concerts)}개")
    
    # schedule.csv에만 있는 타이틀 (전체)
    print("\n" + "=" * 60)
    print("📝 schedule.csv에만 있는 타이틀 (전체)")
    print("=" * 60)
    for i, title in enumerate(sorted(only_in_schedule), 1):
        print(f"{i:2d}. {title}")
    
    # concerts.csv에만 있는 타이틀 (전체)
    print("\n" + "=" * 60)
    print("📝 concerts.csv에만 있는 타이틀 (schedule.csv에 없음)")
    print("=" * 60)
    for i, title in enumerate(sorted(only_in_concerts), 1):
        print(f"{i:2d}. {title}")
    
    # 패턴 분석
    print("\n" + "=" * 60)
    print("🔎 패턴 분석")
    print("=" * 60)
    
    # "콘서트"가 붙은 경우 체크
    concert_suffix = [t for t in only_in_schedule if '콘서트' in t]
    if concert_suffix:
        print(f"\n'콘서트'가 붙은 타이틀 ({len(concert_suffix)}개):")
        for title in concert_suffix:
            # concerts.csv에서 유사한 타이틀 찾기
            base_title = title.replace(' 콘서트', '').replace('콘서트', '')
            similar = [c for c in concerts_titles if base_title in c or c in base_title]
            if similar:
                print(f"  • {title}")
                print(f"    → 유사 타이틀: {similar[0]}")

if __name__ == "__main__":
    show_schedule_mismatches()