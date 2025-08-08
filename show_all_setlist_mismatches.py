#!/usr/bin/env python3
import pandas as pd

def show_all_setlist_mismatches():
    base_path = '/Users/youz2me/Xcode/Livith-Data/output/'
    
    # concerts.csv 타이틀 가져오기
    concerts_df = pd.read_csv(base_path + 'concerts.csv', encoding='utf-8')
    concerts_titles = set(concerts_df['title'].tolist())
    
    # concert_setlists.csv 타이틀 가져오기
    concert_setlists_df = pd.read_csv(base_path + 'concert_setlists.csv', encoding='utf-8')
    concert_setlists_titles = set(concert_setlists_df.iloc[:, 0].tolist())
    
    # concert_setlists.csv에만 있는 타이틀
    only_in_setlists = concert_setlists_titles - concerts_titles
    
    print("=" * 60)
    print("📋 concert_setlists.csv에만 있는 타이틀 (전체)")
    print("=" * 60)
    print(f"\n총 {len(only_in_setlists)}개의 타이틀이 concert_setlists.csv에만 있습니다:\n")
    
    for i, title in enumerate(sorted(only_in_setlists), 1):
        print(f"{i:2d}. {title}")
    
    print("\n" + "=" * 60)
    print("✅ concerts.csv 기준으로 매칭되지 않는 타이틀 리스트 완료")
    print("=" * 60)

if __name__ == "__main__":
    show_all_setlist_mismatches()