#!/usr/bin/env python3
import pandas as pd

def sort_concerts_by_matching():
    try:
        print("파일 읽기...")
        # concerts.csv와 artists.csv 읽기
        concerts_df = pd.read_csv('/Users/youz2me/Xcode/Livith-Data/output/concerts.csv', encoding='utf-8')
        artists_df = pd.read_csv('/Users/youz2me/Xcode/Livith-Data/output/artists.csv', encoding='utf-8', header=None)
        
        # artists.csv의 아티스트 이름들
        artist_names = set(artists_df.iloc[:, 0].tolist())
        
        print(f"artists.csv에 있는 아티스트: {len(artist_names)}개")
        print(f"concerts.csv에 있는 콘서트: {len(concerts_df)}개")
        
        # 매칭 여부 확인하는 함수
        def is_matched(artist_name):
            return artist_name in artist_names
        
        # 매칭 여부 컬럼 추가
        concerts_df['is_matched'] = concerts_df['artist'].apply(is_matched)
        
        # 매칭된 것과 안된 것 분리
        matched_df = concerts_df[concerts_df['is_matched'] == True].copy()
        unmatched_df = concerts_df[concerts_df['is_matched'] == False].copy()
        
        print(f"매칭된 콘서트: {len(matched_df)}개")
        print(f"매칭되지 않은 콘서트: {len(unmatched_df)}개")
        
        # 각각 아티스트명으로 정렬
        matched_df_sorted = matched_df.sort_values('artist')
        unmatched_df_sorted = unmatched_df.sort_values('artist')
        
        # is_matched 컬럼 제거
        matched_df_sorted = matched_df_sorted.drop('is_matched', axis=1)
        unmatched_df_sorted = unmatched_df_sorted.drop('is_matched', axis=1)
        
        # 매칭된 것을 위로, 안된 것을 아래로 합치기
        final_df = pd.concat([matched_df_sorted, unmatched_df_sorted], ignore_index=True)
        
        # 저장
        final_df.to_csv('/Users/youz2me/Xcode/Livith-Data/output/concerts.csv', 
                       index=False, encoding='utf-8')
        
        print("\n✅ concerts.csv 정렬 완료!")
        print(f"매칭된 아티스트들이 위에, 매칭되지 않은 아티스트들이 아래에 정렬되었습니다.")
        
        # 결과 미리보기
        print("\n매칭된 아티스트들 (상위 10개):")
        print(matched_df_sorted[['artist', 'title']].head(10))
        
        print("\n매칭되지 않은 아티스트들 (상위 10개):")
        print(unmatched_df_sorted[['artist', 'title']].head(10))
        
        return True
        
    except Exception as e:
        print(f"오류 발생: {e}")
        return False

if __name__ == "__main__":
    success = sort_concerts_by_matching()
    if success:
        print("\n✅ 정렬 완료!")
    else:
        print("\n❌ 정렬 실패!")