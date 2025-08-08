#!/usr/bin/env python3
import pandas as pd
import os

def update_setlists_to_match():
    """setlists.csv를 concert_setlists.csv와 매칭되도록 업데이트"""
    
    base_path = '/Users/youz2me/Xcode/Livith-Data/output'
    setlists_path = os.path.join(base_path, 'setlists.csv')
    concert_setlists_path = os.path.join(base_path, 'concert_setlists.csv')
    
    # 파일 읽기
    concert_setlists_df = pd.read_csv(concert_setlists_path)
    
    # concert_setlists에서 고유한 setlist_title 추출
    unique_setlist_titles = concert_setlists_df['setlist_title'].unique()
    
    # 새로운 setlists 데이터 생성
    new_setlists = []
    for title in unique_setlist_titles:
        # concert_setlists에서 해당 타이틀의 첫 번째 행 가져오기
        concert_row = concert_setlists_df[concert_setlists_df['setlist_title'] == title].iloc[0]
        concert_title = concert_row['concert_title']
        
        # 기본 데이터 생성
        new_setlist = {
            'title': title,  # concert_setlists의 setlist_title과 동일
            'start_date': '2025-01-01',  # 실제 날짜로 업데이트 필요
            'end_date': '2025-01-01',    # 실제 날짜로 업데이트 필요
            'img_url': '',
            'artist': concert_title.split(' LIVE TOUR')[0] if 'LIVE TOUR' in concert_title else concert_title.split('[')[0].strip(),
            'venue': ''
        }
        new_setlists.append(new_setlist)
    
    # 새로운 DataFrame 생성
    new_setlists_df = pd.DataFrame(new_setlists)
    
    # CSV 저장
    new_setlists_df.to_csv(setlists_path, index=False, encoding='utf-8-sig')
    
    print("setlists.csv 업데이트 완료:")
    print("=" * 60)
    print("새로운 setlists.csv의 title:")
    for title in new_setlists_df['title'].tolist():
        print(f"  - {title}")
    
    print("\n" + "=" * 60)
    print("매칭 확인:")
    for title in new_setlists_df['title'].tolist():
        if title in concert_setlists_df['setlist_title'].tolist():
            print(f"✅ '{title}' - concert_setlists.csv와 매칭됨")
        else:
            print(f"❌ '{title}' - 매칭 안됨")

if __name__ == "__main__":
    update_setlists_to_match()