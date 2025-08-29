#!/usr/bin/env python3
import pandas as pd

def analyze_setlists_artists():
    base_path = '/Users/youz2me/Xcode/Livith-Data/output/'
    
    print("=" * 80)
    print("🎭 Setlists.csv Artist 이름 분석")
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
    
    # concerts.csv 아티스트 목록
    concerts_artists = set(concerts_df['artist'].tolist())
    
    # setlists.csv 아티스트 목록
    setlists_artists = set(setlists_df['artist'].unique())
    
    print(f"\n📊 전체 현황:")
    print(f"  • setlists.csv 고유 아티스트: {len(setlists_artists)}개")
    print(f"  • artists.csv 표준 아티스트: {len(standard_artists)}개")
    print(f"  • concerts.csv 아티스트: {len(concerts_artists)}개")
    
    # 매칭 분석
    matched_with_artists = setlists_artists & standard_artists
    matched_with_concerts = setlists_artists & concerts_artists
    unmatched_with_artists = setlists_artists - standard_artists
    unmatched_with_concerts = setlists_artists - concerts_artists
    
    print(f"\n📈 매칭 결과:")
    print(f"  ✅ artists.csv와 매칭: {len(matched_with_artists)}/{len(setlists_artists)} ({len(matched_with_artists)*100/len(setlists_artists):.1f}%)")
    print(f"  ✅ concerts.csv와 매칭: {len(matched_with_concerts)}/{len(setlists_artists)} ({len(matched_with_concerts)*100/len(setlists_artists):.1f}%)")
    
    # 매칭되지 않은 아티스트들 상세 분석
    if unmatched_with_artists:
        print(f"\n🚨 artists.csv와 매칭되지 않은 아티스트들 ({len(unmatched_with_artists)}개):")
        print("-" * 80)
        
        for i, artist in enumerate(sorted(unmatched_with_artists), 1):
            print(f"\n{i}. '{artist}'")
            
            # 이 아티스트의 셋리스트 개수
            setlist_count = len(setlists_df[setlists_df['artist'] == artist])
            print(f"   📝 셋리스트 수: {setlist_count}개")
            
            # 유사한 이름이 있는지 확인
            similar_in_artists = []
            similar_in_concerts = []
            
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
            
            # 셋리스트 이름들 확인
            setlist_titles = setlists_df[setlists_df['artist'] == artist]['title'].tolist()
            print(f"   📋 셋리스트 제목:")
            for title in setlist_titles[:3]:  # 처음 3개만 표시
                print(f"      • {title}")
            if len(setlist_titles) > 3:
                print(f"      ... 외 {len(setlist_titles) - 3}개")
                
            if not similar_in_artists and not similar_in_concerts:
                print(f"   ❌ 매핑 정보 없음 - 수동 확인 필요")
    
    # 형식 분석
    print(f"\n🔍 아티스트 이름 형식 분석:")
    print("-" * 80)
    
    # 괄호가 있는 아티스트 vs 없는 아티스트
    with_parentheses = [a for a in setlists_artists if '(' in a and ')' in a]
    without_parentheses = [a for a in setlists_artists if '(' not in a or ')' not in a]
    
    print(f"  • 괄호 포함 (한국어 (원어) 형식): {len(with_parentheses)}개")
    print(f"  • 괄호 없음: {len(without_parentheses)}개")
    
    if without_parentheses:
        print(f"\n  📝 괄호 없는 아티스트들:")
        for artist in sorted(without_parentheses)[:10]:
            print(f"     • {artist}")
        if len(without_parentheses) > 10:
            print(f"     ... 외 {len(without_parentheses) - 10}개")
    
    # 표준 형식과 다른 것들
    non_standard_format = []
    for artist in setlists_artists:
        if '(' in artist and ')' in artist:
            # 괄호 앞뒤 체크
            parts = artist.split('(')
            if len(parts) == 2:
                korean_part = parts[0].strip()
                english_part = parts[1].replace(')', '').strip()
                # 한글이 포함되어 있는지 확인
                if not any('\uac00' <= c <= '\ud7a3' for c in korean_part) and korean_part:
                    non_standard_format.append(artist)
    
    if non_standard_format:
        print(f"\n  ⚠️ 비표준 형식 (원어 (한국어) 형태): {len(non_standard_format)}개")
        for artist in sorted(non_standard_format):
            print(f"     • {artist}")
    
    print("\n" + "=" * 80)
    print("✅ 분석 완료")
    print("=" * 80)

if __name__ == "__main__":
    analyze_setlists_artists()