#!/usr/bin/env python3
import pandas as pd
import os

def final_check_all_files():
    base_path = '/Users/youz2me/Xcode/Livith-Data/output/'
    
    print("=" * 80)
    print("🔍 최종 Foreign Key 검증 - 모든 CSV 파일")
    print("=" * 80)
    
    # 1. artists.csv와 concerts.csv의 artist 필드 검사
    print("\n" + "─" * 80)
    print("1️⃣ ARTIST FOREIGN KEY 검사 (artists.csv ↔ concerts.csv)")
    print("─" * 80)
    
    try:
        artists_df = pd.read_csv(base_path + 'artists.csv', encoding='utf-8')
        concerts_df = pd.read_csv(base_path + 'concerts.csv', encoding='utf-8')
        
        # artists.csv의 artist 컬럼
        if 'artist' in artists_df.columns:
            artists_names = set(artists_df['artist'].tolist())
        else:
            artists_names = set(artists_df.iloc[:, 0].tolist())
            
        # concerts.csv의 artist 컬럼
        concerts_artists = set(concerts_df['artist'].tolist())
        
        matched_artists = artists_names & concerts_artists
        only_in_artists = artists_names - concerts_artists
        only_in_concerts = concerts_artists - artists_names
        
        print(f"  📁 artists.csv: {len(artists_names)}개 아티스트")
        print(f"  📁 concerts.csv: {len(concerts_artists)}개 아티스트")
        print(f"  ✅ 매칭: {len(matched_artists)}개")
        
        if only_in_artists:
            print(f"  ⚠️ artists.csv에만: {len(only_in_artists)}개")
            for artist in list(only_in_artists)[:5]:
                print(f"     • {artist}")
                
        if only_in_concerts:
            print(f"  ⚠️ concerts.csv에만: {len(only_in_concerts)}개")
            for artist in list(only_in_concerts)[:5]:
                print(f"     • {artist}")
    except Exception as e:
        print(f"  ❌ 오류: {e}")
    
    # 2. concerts.csv title을 기준으로 다른 파일들 검사
    concerts_titles = set(concerts_df['title'].tolist())
    print(f"\n📌 기준: concerts.csv의 title {len(concerts_titles)}개")
    
    # 파일 목록
    files_to_check = [
        ('concert_info.csv', 0),  # 첫 번째 컬럼이 concert_title
        ('concert_setlists.csv', 0),  # 첫 번째 컬럼이 concert_title
        ('cultures.csv', 0),  # 첫 번째 컬럼이 concert_title
        ('schedule.csv', 'concert_title'),  # concert_title 컬럼명
        ('setlists.csv', 'concert_title'),  # concert_title 컬럼
        ('setlist_songs.csv', 'concert_title'),  # concert_title 컬럼
        ('songs.csv', None),  # concert_title 없음
    ]
    
    for file_name, title_col in files_to_check:
        print("\n" + "─" * 80)
        print(f"📁 {file_name} 검사")
        print("─" * 80)
        
        file_path = base_path + file_name
        if not os.path.exists(file_path):
            print(f"  ⚠️ 파일이 존재하지 않습니다.")
            continue
            
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            print(f"  • 총 행 수: {len(df)}")
            print(f"  • 컬럼: {', '.join(df.columns[:5])}...")
            
            # concert_title 관련 컬럼 찾기
            if title_col is None:
                print(f"  • concert_title 컬럼 없음 (정상)")
                continue
            elif isinstance(title_col, str):
                if title_col in df.columns:
                    file_titles = set(df[title_col].dropna().tolist())
                else:
                    print(f"  ⚠️ '{title_col}' 컬럼이 없습니다.")
                    continue
            else:
                # 인덱스로 접근
                file_titles = set(df.iloc[:, title_col].dropna().tolist())
            
            # 매칭 분석
            matched = file_titles & concerts_titles
            only_in_file = file_titles - concerts_titles
            missing_from_file = concerts_titles - file_titles
            
            print(f"  • 고유 title 수: {len(file_titles)}")
            print(f"  ✅ 매칭: {len(matched)}/{len(file_titles)} ({len(matched)*100/len(file_titles):.1f}%)")
            
            if only_in_file:
                print(f"  ⚠️ {file_name}에만 있음: {len(only_in_file)}개")
                for title in list(only_in_file)[:3]:
                    print(f"     • {title}")
                    
            if missing_from_file and len(missing_from_file) < 10:
                print(f"  ℹ️ concerts.csv에만 있음: {len(missing_from_file)}개")
                
        except Exception as e:
            print(f"  ❌ 오류: {e}")
    
    # 3. 아티스트 관련 추가 체크
    print("\n" + "─" * 80)
    print("🎤 ARTIST 관련 추가 검사")
    print("─" * 80)
    
    # songs.csv의 artist 체크
    try:
        songs_df = pd.read_csv(base_path + 'songs.csv', encoding='utf-8')
        if 'artist' in songs_df.columns:
            songs_artists = set(songs_df['artist'].unique())
            matched_with_artists = songs_artists & artists_names
            print(f"  📁 songs.csv 아티스트: {len(songs_artists)}개")
            print(f"  ✅ artists.csv와 매칭: {len(matched_with_artists)}/{len(songs_artists)}")
    except Exception as e:
        print(f"  ❌ songs.csv 오류: {e}")
    
    # setlists.csv의 artist 체크
    try:
        setlists_df = pd.read_csv(base_path + 'setlists.csv', encoding='utf-8')
        if 'artist' in setlists_df.columns:
            setlist_artists = set(setlists_df['artist'].unique())
            matched_with_artists = setlist_artists & artists_names
            print(f"  📁 setlists.csv 아티스트: {len(setlist_artists)}개")
            print(f"  ✅ artists.csv와 매칭: {len(matched_with_artists)}/{len(setlist_artists)}")
    except Exception as e:
        print(f"  ❌ setlists.csv 오류: {e}")
    
    print("\n" + "=" * 80)
    print("✅ 전체 검사 완료")
    print("=" * 80)

if __name__ == "__main__":
    final_check_all_files()