#!/usr/bin/env python3
import pandas as pd

def standardize_songs_artists():
    base_path = '/Users/youz2me/Xcode/Livith-Data/output/'
    
    print("=" * 60)
    print("🎵 Songs.csv Artist 표준화 작업")
    print("=" * 60)
    
    # 1. 필요한 파일들 읽기
    print("\n1. 파일 읽기...")
    songs_df = pd.read_csv(base_path + 'songs.csv', encoding='utf-8')
    concerts_df = pd.read_csv(base_path + 'concerts.csv', encoding='utf-8')
    setlists_df = pd.read_csv(base_path + 'setlists.csv', encoding='utf-8')
    setlist_songs_df = pd.read_csv(base_path + 'setlist_songs.csv', encoding='utf-8')
    
    print(f"  • songs.csv: {len(songs_df)}개 곡")
    print(f"  • concerts.csv: {len(concerts_df)}개 콘서트")
    print(f"  • setlists.csv: {len(setlists_df)}개 셋리스트")
    print(f"  • setlist_songs.csv: {len(setlist_songs_df)}개 항목")
    
    # 2. setlist_title → concert_title → artist 매핑 생성
    print("\n2. 매핑 테이블 생성...")
    
    # setlists.csv에서 title과 concert_title 매핑
    setlist_to_concert = {}
    for _, row in setlists_df.iterrows():
        setlist_title = row['title']
        concert_title = row['concert_title']
        setlist_to_concert[setlist_title] = concert_title
    
    # concerts.csv에서 concert_title과 artist 매핑
    concert_to_artist = {}
    for _, row in concerts_df.iterrows():
        concert_title = row['title']
        artist = row['artist']
        concert_to_artist[concert_title] = artist
    
    # setlist_title → artist 직접 매핑
    setlist_to_artist = {}
    for setlist_title, concert_title in setlist_to_concert.items():
        if concert_title in concert_to_artist:
            setlist_to_artist[setlist_title] = concert_to_artist[concert_title]
    
    print(f"  • setlist → concert 매핑: {len(setlist_to_concert)}개")
    print(f"  • concert → artist 매핑: {len(concert_to_artist)}개")
    print(f"  • setlist → artist 매핑: {len(setlist_to_artist)}개")
    
    # 3. songs.csv의 각 곡이 어떤 setlist에 속하는지 확인
    print("\n3. Songs와 Setlist 연결...")
    
    # song_title과 setlist_title 매핑
    song_to_setlist = {}
    for _, row in setlist_songs_df.iterrows():
        song_title = row['song_title']
        setlist_title = row['setlist_title']
        if song_title not in song_to_setlist:
            song_to_setlist[song_title] = []
        song_to_setlist[song_title].append(setlist_title)
    
    # 4. songs.csv의 artist 업데이트
    print("\n4. Artist 이름 표준화...")
    
    # 기존 artist 이름들 확인
    original_artists = songs_df['artist'].unique()
    print(f"  • 기존 고유 artist: {len(original_artists)}개")
    
    # artist 매핑 생성
    artist_mapping = {}
    updated_count = 0
    
    for original_artist in original_artists:
        # 해당 artist의 곡들 찾기
        artist_songs = songs_df[songs_df['artist'] == original_artist]['title'].tolist()
        
        # 이 곡들이 속한 setlist 찾기
        found_artists = set()
        for song_title in artist_songs:
            if song_title in song_to_setlist:
                for setlist_title in song_to_setlist[song_title]:
                    if setlist_title in setlist_to_artist:
                        found_artists.add(setlist_to_artist[setlist_title])
        
        # 가장 많이 나타난 표준 artist 이름 선택
        if found_artists:
            # 하나만 있으면 그것 사용
            if len(found_artists) == 1:
                standard_artist = list(found_artists)[0]
                artist_mapping[original_artist] = standard_artist
                if original_artist != standard_artist:
                    updated_count += 1
                    print(f"  ✓ '{original_artist}' → '{standard_artist}'")
            else:
                # 여러 개면 첫 번째 것 사용 (또는 더 정교한 로직 필요)
                standard_artist = sorted(list(found_artists))[0]
                artist_mapping[original_artist] = standard_artist
                print(f"  ⚠️ '{original_artist}' → 여러 매칭 중 '{standard_artist}' 선택")
        else:
            # 매핑을 찾을 수 없으면 원본 유지
            artist_mapping[original_artist] = original_artist
    
    # 5. songs.csv 업데이트
    print(f"\n5. Songs.csv 업데이트...")
    songs_df['artist'] = songs_df['artist'].map(artist_mapping)
    
    # 저장
    songs_df.to_csv(base_path + 'songs.csv', index=False, encoding='utf-8')
    
    print(f"  ✅ {updated_count}개의 artist 이름이 표준화되었습니다.")
    
    # 6. 결과 확인
    print("\n6. 최종 결과:")
    new_artists = songs_df['artist'].unique()
    print(f"  • 표준화 후 고유 artist: {len(new_artists)}개")
    
    # artists.csv와 비교
    artists_df = pd.read_csv(base_path + 'artists.csv', encoding='utf-8')
    if 'artist' in artists_df.columns:
        standard_artists = set(artists_df['artist'].tolist())
    else:
        standard_artists = set(artists_df.iloc[:, 0].tolist())
    
    matched = set(new_artists) & standard_artists
    print(f"  • artists.csv와 매칭: {len(matched)}/{len(new_artists)}개")
    
    if len(matched) < len(new_artists):
        unmatched = set(new_artists) - standard_artists
        print(f"\n  ⚠️ 매칭되지 않은 artist:")
        for artist in sorted(unmatched)[:10]:
            print(f"     • {artist}")

if __name__ == "__main__":
    standardize_songs_artists()