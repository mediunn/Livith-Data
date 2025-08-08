#!/usr/bin/env python3
import pandas as pd

def remove_yama():
    base_path = '/Users/youz2me/Xcode/Livith-Data/output/'
    
    print("=" * 60)
    print("🗑️ yama 아티스트 삭제")
    print("=" * 60)
    
    # songs.csv 읽기
    songs_df = pd.read_csv(base_path + 'songs.csv', encoding='utf-8')
    
    # yama 관련 곡들 확인
    yama_songs = songs_df[songs_df['artist'] == 'yama']
    print(f"삭제 전 총 곡 수: {len(songs_df)}")
    print(f"yama 곡 수: {len(yama_songs)}")
    
    if len(yama_songs) > 0:
        print(f"\n삭제될 yama 곡들:")
        for i, (_, row) in enumerate(yama_songs.iterrows(), 1):
            print(f"  {i}. {row['title']}")
    
    # yama가 아닌 곡들만 유지
    songs_df_filtered = songs_df[songs_df['artist'] != 'yama'].copy()
    
    print(f"\n삭제 후 총 곡 수: {len(songs_df_filtered)}")
    print(f"삭제된 곡 수: {len(songs_df) - len(songs_df_filtered)}")
    
    # 저장
    songs_df_filtered.to_csv(base_path + 'songs.csv', index=False, encoding='utf-8')
    
    # 최종 아티스트 수 확인
    remaining_artists = len(songs_df_filtered['artist'].unique())
    print(f"남은 고유 아티스트: {remaining_artists}개")
    
    print("\n" + "=" * 60)
    print("✅ yama 삭제 완료")
    print("=" * 60)

if __name__ == "__main__":
    remove_yama()