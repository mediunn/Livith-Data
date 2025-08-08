#!/usr/bin/env python3
import pandas as pd

def fix_setlists_artists():
    base_path = '/Users/youz2me/Xcode/Livith-Data/output/'
    
    print("=" * 80)
    print("🔧 Setlists.csv 아티스트 이름 표준화")
    print("=" * 80)
    
    # 파일들 읽기
    setlists_df = pd.read_csv(base_path + 'setlists.csv', encoding='utf-8')
    artists_df = pd.read_csv(base_path + 'artists.csv', encoding='utf-8')
    concerts_df = pd.read_csv(base_path + 'concerts.csv', encoding='utf-8')
    
    # 표준 아티스트 목록
    if 'artist' in artists_df.columns:
        standard_artists = set(artists_df['artist'].tolist())
    else:
        standard_artists = set(artists_df.iloc[:, 0].tolist())
    
    print(f"수정 전 setlists.csv 고유 아티스트: {len(setlists_df['artist'].unique())}개")
    
    # concerts.csv에서 표준 매핑 생성
    concerts_artist_mapping = {}
    for _, row in concerts_df.iterrows():
        concerts_artist_mapping[row['artist']] = row['artist']  # 이미 표준화된 것
    
    # 수정 매핑 생성
    corrections = {}
    
    # 1. 호시노 겐 수정
    corrections['호시노 겐'] = '호시노 겐 (星野源)'
    
    # 2. 형식이 맞지 않는 아티스트들 수정 (원어 (한국어) → 한국어 (원어))
    # concerts.csv/artists.csv에서 올바른 형식 찾아서 매핑
    
    current_artists = setlists_df['artist'].unique()
    
    for current_artist in current_artists:
        if current_artist == '호시노 겐':
            continue  # 이미 위에서 처리
            
        # concerts.csv에서 일치하는 항목 찾기
        matching_standard = None
        
        # 정확히 일치하는 것이 있는지 먼저 확인
        if current_artist in standard_artists:
            continue  # 이미 표준 형식
            
        # 단어 기반으로 매칭 시도
        if '(' in current_artist and ')' in current_artist:
            # 현재가 "원어 (한국어)" 형식인 경우
            parts = current_artist.split('(')
            if len(parts) == 2:
                english_part = parts[0].strip()
                korean_part = parts[1].replace(')', '').strip()
                
                # "한국어 (원어)" 형태의 표준 아티스트 찾기
                expected_standard = f"{korean_part} ({english_part})"
                if expected_standard in standard_artists:
                    corrections[current_artist] = expected_standard
                    continue
        
        # 부분 매칭 시도
        current_words = current_artist.lower().replace('(', ' ').replace(')', ' ').split()
        for std_artist in standard_artists:
            std_words = std_artist.lower().replace('(', ' ').replace(')', ' ').split()
            # 공통 단어 비율이 높으면 매칭
            common_words = set(current_words) & set(std_words)
            if len(common_words) >= max(1, min(len(current_words), len(std_words)) // 2):
                corrections[current_artist] = std_artist
                break
    
    # 수정 적용
    print(f"\n🔧 수정 적용:")
    correction_count = 0
    
    for old_name, new_name in corrections.items():
        old_count = len(setlists_df[setlists_df['artist'] == old_name])
        setlists_df.loc[setlists_df['artist'] == old_name, 'artist'] = new_name
        print(f"  ✓ '{old_name}' → '{new_name}' ({old_count}개 셋리스트)")
        correction_count += 1
    
    # 저장
    setlists_df.to_csv(base_path + 'setlists.csv', index=False, encoding='utf-8')
    
    print(f"\n수정 후 고유 아티스트: {len(setlists_df['artist'].unique())}개")
    print(f"수정된 아티스트: {correction_count}개")
    
    # 최종 매칭 확인
    final_artists = set(setlists_df['artist'].unique())
    matched = final_artists & standard_artists
    unmatched = final_artists - standard_artists
    
    print(f"\n📊 최종 매칭 결과:")
    print(f"  • 매칭: {len(matched)}/{len(final_artists)} ({len(matched)*100/len(final_artists):.1f}%)")
    
    if unmatched:
        print(f"  • 여전히 매칭 안됨: {len(unmatched)}개")
        for artist in sorted(unmatched):
            print(f"     - {artist}")
    
    print("\n" + "=" * 80)
    print("✅ 수정 완료")
    print("=" * 80)

if __name__ == "__main__":
    fix_setlists_artists()