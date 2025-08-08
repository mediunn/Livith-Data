#!/usr/bin/env python3
import pandas as pd

def analyze_songs_mismatch():
    base_path = '/Users/youz2me/Xcode/Livith-Data/output/'
    
    print("=" * 80)
    print("🔍 Songs.csv 매칭 문제 분석")
    print("=" * 80)
    
    # 파일들 읽기
    songs_df = pd.read_csv(base_path + 'songs.csv', encoding='utf-8')
    artists_df = pd.read_csv(base_path + 'artists.csv', encoding='utf-8')
    concerts_df = pd.read_csv(base_path + 'concerts.csv', encoding='utf-8')
    
    # artists.csv의 표준 아티스트 목록
    if 'artist' in artists_df.columns:
        standard_artists = set(artists_df['artist'].tolist())
    else:
        standard_artists = set(artists_df.iloc[:, 0].tolist())
    
    # songs.csv의 아티스트 목록
    songs_artists = set(songs_df['artist'].unique())
    
    # 매칭되지 않은 아티스트들
    unmatched = songs_artists - standard_artists
    matched = songs_artists & standard_artists
    
    print(f"\n📊 전체 현황:")
    print(f"  • songs.csv 고유 아티스트: {len(songs_artists)}개")
    print(f"  • artists.csv 표준 아티스트: {len(standard_artists)}개")
    print(f"  • 매칭: {len(matched)}개")
    print(f"  • 매칭 안됨: {len(unmatched)}개")
    
    print(f"\n🚨 매칭되지 않은 아티스트들 상세 분석:")
    print("-" * 80)
    
    for i, artist in enumerate(sorted(unmatched), 1):
        print(f"\n{i}. '{artist}'")
        
        # 이 아티스트의 곡 개수
        song_count = len(songs_df[songs_df['artist'] == artist])
        print(f"   📀 곡 수: {song_count}개")
        
        # 유사한 이름이 artists.csv나 concerts.csv에 있는지 확인
        similar_in_artists = []
        similar_in_concerts = []
        
        concerts_artists = set(concerts_df['artist'].tolist())
        
        # 단어 기준으로 유사성 확인
        artist_words = artist.lower().replace('(', ' ').replace(')', ' ').split()
        
        for std_artist in standard_artists:
            std_words = std_artist.lower().replace('(', ' ').replace(')', ' ').split()
            # 공통 단어가 있으면 유사한 것으로 간주
            if any(word in std_words for word in artist_words if len(word) > 2):
                similar_in_artists.append(std_artist)
        
        for concert_artist in concerts_artists:
            concert_words = concert_artist.lower().replace('(', ' ').replace(')', ' ').split()
            if any(word in concert_words for word in artist_words if len(word) > 2):
                if concert_artist not in similar_in_concerts:
                    similar_in_concerts.append(concert_artist)
        
        if similar_in_artists:
            print(f"   🎯 artists.csv에서 유사: {similar_in_artists}")
        
        if similar_in_concerts:
            print(f"   🎪 concerts.csv에서 유사: {similar_in_concerts}")
        
        # 이 아티스트의 곡들이 어떤 셋리스트에 속하는지 확인
        artist_songs = songs_df[songs_df['artist'] == artist]['title'].tolist()
        
        # setlist_songs에서 이 곡들 찾기
        setlist_songs_df = pd.read_csv(base_path + 'setlist_songs.csv', encoding='utf-8')
        found_setlists = []
        
        for song_title in artist_songs[:5]:  # 처음 5개 곡만 확인
            matching_setlists = setlist_songs_df[setlist_songs_df['song_title'] == song_title]['setlist_title'].tolist()
            found_setlists.extend(matching_setlists)
        
        if found_setlists:
            unique_setlists = list(set(found_setlists))
            print(f"   📝 속한 셋리스트: {len(unique_setlists)}개")
            for setlist in unique_setlists[:3]:  # 처음 3개만 표시
                print(f"      • {setlist}")
            
            # 이 셋리스트들이 어떤 콘서트와 연결되는지 확인
            concert_setlists_df = pd.read_csv(base_path + 'concert_setlists.csv', encoding='utf-8')
            related_concerts = []
            
            for setlist_title in unique_setlists:
                concerts = concert_setlists_df[concert_setlists_df['setlist_title'] == setlist_title]['concert_title'].tolist()
                related_concerts.extend(concerts)
            
            if related_concerts:
                unique_concerts = list(set(related_concerts))
                print(f"   🎪 연결된 콘서트: {len(unique_concerts)}개")
                for concert in unique_concerts[:3]:
                    # 이 콘서트의 아티스트 찾기
                    concert_artist = concerts_df[concerts_df['title'] == concert]['artist'].tolist()
                    if concert_artist:
                        print(f"      • {concert} → {concert_artist[0]}")
                        
        if not similar_in_artists and not similar_in_concerts and not found_setlists:
            print(f"   ❌ 매핑 정보 없음 - 수동 확인 필요")
    
    print("\n" + "=" * 80)
    print("✅ 분석 완료")
    print("=" * 80)

if __name__ == "__main__":
    analyze_songs_mismatch()