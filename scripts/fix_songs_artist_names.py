#!/usr/bin/env python3
import pandas as pd

def fix_songs_artist_names():
    base_path = '/Users/youz2me/Xcode/Livith-Data/output/'
    
    print("=" * 60)
    print("🔧 Songs.csv 아티스트 이름 수정")
    print("=" * 60)
    
    # songs.csv 읽기
    songs_df = pd.read_csv(base_path + 'songs.csv', encoding='utf-8')
    
    print(f"수정 전 고유 아티스트: {len(songs_df['artist'].unique())}개")
    
    # 수정 매핑
    corrections = {
        'SCANDAL': 'SCANDAL (스캔달)',
        '스즈키 코노미': 'Suzuki Konomi (스즈키 코노미)',
        '오아시스(Oasis)': 'Oasis (오아시스)',
        '제임스 블레이크': 'James Blake (제임스 블레이크)',
        '크리스토퍼(Christopher)': 'Christopher (크리스토퍼)',
        '타카네노나데시코(TAKANE NO NADESHIKO)': 'TAKANE NO NADESHIKO (타카네노나데시코)',
        '폼파돌스 (PompadollS) 라이브': 'PompadollS (폼파돌스)'
    }
    
    # yama의 경우 별도 확인
    print("\n🔍 yama 아티스트 확인:")
    yama_songs = songs_df[songs_df['artist'] == 'yama']['title'].tolist()
    print(f"  • yama 곡 수: {len(yama_songs)}")
    print(f"  • 대표 곡: {yama_songs[:3]}")
    
    # concerts.csv에 yama가 있는지 확인
    concerts_df = pd.read_csv(base_path + 'concerts.csv', encoding='utf-8')
    yama_concerts = concerts_df[concerts_df['artist'].str.contains('yama', case=False, na=False)]
    
    if len(yama_concerts) > 0:
        print(f"  • concerts.csv에서 yama 관련: {yama_concerts['artist'].tolist()}")
        # 첫 번째 매칭되는 아티스트로 설정
        corrections['yama'] = yama_concerts.iloc[0]['artist']
    else:
        print("  • concerts.csv에서 yama 관련 아티스트를 찾을 수 없음")
        # artists.csv에서 직접 확인
        artists_df = pd.read_csv(base_path + 'artists.csv', encoding='utf-8')
        if 'artist' in artists_df.columns:
            yama_artists = artists_df[artists_df['artist'].str.contains('yama', case=False, na=False)]
        else:
            yama_artists = artists_df[artists_df.iloc[:, 0].str.contains('yama', case=False, na=False)]
        
        if len(yama_artists) > 0:
            if 'artist' in artists_df.columns:
                corrections['yama'] = yama_artists.iloc[0]['artist']
            else:
                corrections['yama'] = yama_artists.iloc[0, 0]
            print(f"  • artists.csv에서 발견: {corrections['yama']}")
    
    # 수정 적용
    print(f"\n🔧 수정 적용:")
    for old_name, new_name in corrections.items():
        old_count = len(songs_df[songs_df['artist'] == old_name])
        songs_df.loc[songs_df['artist'] == old_name, 'artist'] = new_name
        print(f"  ✓ '{old_name}' → '{new_name}' ({old_count}곡)")
    
    # 저장
    songs_df.to_csv(base_path + 'songs.csv', index=False, encoding='utf-8')
    
    print(f"\n수정 후 고유 아티스트: {len(songs_df['artist'].unique())}개")
    
    # 최종 매칭 확인
    artists_df = pd.read_csv(base_path + 'artists.csv', encoding='utf-8')
    if 'artist' in artists_df.columns:
        standard_artists = set(artists_df['artist'].tolist())
    else:
        standard_artists = set(artists_df.iloc[:, 0].tolist())
    
    songs_artists = set(songs_df['artist'].unique())
    matched = songs_artists & standard_artists
    unmatched = songs_artists - standard_artists
    
    print(f"\n📊 최종 매칭 결과:")
    print(f"  • 매칭: {len(matched)}/{len(songs_artists)} ({len(matched)*100/len(songs_artists):.1f}%)")
    
    if unmatched:
        print(f"  • 여전히 매칭 안됨: {len(unmatched)}개")
        for artist in sorted(unmatched):
            print(f"     - {artist}")
    
    print("\n" + "=" * 60)
    print("✅ 수정 완료")
    print("=" * 60)

if __name__ == "__main__":
    fix_songs_artist_names()