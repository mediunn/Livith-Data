#!/usr/bin/env python3
import pandas as pd

def compare_artist_files():
    try:
        # 파일 읽기 (artists.csv는 헤더 없음)
        artists_df = pd.read_csv('/Users/youz2me/Xcode/Livith-Data/output/artists.csv', encoding='utf-8', header=None)
        concerts_df = pd.read_csv('/Users/youz2me/Xcode/Livith-Data/output/concerts.csv', encoding='utf-8', header=None)
        
        # artists.csv에서 아티스트 이름 목록 가져오기 (첫 번째 컬럼)
        artists_list = set(artists_df.iloc[:, 0].tolist())
        
        # concerts.csv에서 아티스트 이름 목록 가져오기 (첫 번째 컬럼)
        concerts_list = set(concerts_df.iloc[:, 0].tolist())
        
        print(f"artists.csv에 있는 아티스트 수: {len(artists_list)}")
        print(f"concerts.csv에 있는 아티스트 수: {len(concerts_list)}")
        
        # concerts.csv에만 있는 아티스트 (artists.csv에 없음)
        only_in_concerts = concerts_list - artists_list
        
        # artists.csv에만 있는 아티스트 (concerts.csv에 없음)  
        only_in_artists = artists_list - concerts_list
        
        # 두 파일 모두에 있는 아티스트
        in_both = artists_list & concerts_list
        
        print(f"\n두 파일 모두에 있는 아티스트: {len(in_both)}개")
        
        if only_in_concerts:
            print(f"\nconcerts.csv에만 있는 아티스트: {len(only_in_concerts)}개")
            for artist in sorted(only_in_concerts):
                print(f"  - {artist}")
        
        if only_in_artists:
            print(f"\nartists.csv에만 있는 아티스트: {len(only_in_artists)}개")
            for artist in sorted(only_in_artists):
                print(f"  - {artist}")
        
        if not only_in_concerts and not only_in_artists:
            print("\n✅ 모든 아티스트가 두 파일에 모두 존재합니다!")
        else:
            print(f"\n⚠️  불일치 발견: concerts에만 {len(only_in_concerts)}개, artists에만 {len(only_in_artists)}개")
        
    except Exception as e:
        print(f"오류 발생: {e}")

if __name__ == "__main__":
    compare_artist_files()