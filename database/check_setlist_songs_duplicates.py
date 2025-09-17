#!/usr/bin/env python3
"""
setlist_songs.csv의 중복 확인 및 제거 스크립트
"""
import pandas as pd

def check_setlist_songs_duplicates():
    """setlist_songs.csv의 중복 확인"""
    
    print("🔍 setlist_songs.csv 중복 확인 시작")
    print("=" * 60)
    
    # setlist_songs.csv 파일 읽기
    setlist_songs_path = '/Users/youz2me/Xcode/Livith-Data/output/cleaned_data/setlist_songs.csv'
    
    print("📁 setlist_songs.csv 파일 로딩 중...")
    df = pd.read_csv(setlist_songs_path)
    
    print(f"• 원본 레코드 수: {len(df)}개")
    
    # 1. 전체 중복 (모든 컬럼 동일)
    print(f"\n🔍 1. 전체 중복 (모든 컬럼 동일) 확인:")
    full_duplicates = df.duplicated().sum()
    print(f"• 완전 중복 레코드: {full_duplicates}개")
    
    if full_duplicates > 0:
        print("• 완전 중복 레코드 샘플:")
        duplicate_rows = df[df.duplicated(keep=False)].head(5)
        for i, (_, row) in enumerate(duplicate_rows.iterrows()):
            print(f"  {i+1}. Setlist {row['setlist_id']}, Order {row['order_index']}: {row['song_title']}")
    
    # 2. setlist_id + order_index 중복 (UNIQUE KEY 위반)
    print(f"\n🔍 2. setlist_id + order_index 중복 확인:")
    position_duplicates = df.groupby(['setlist_id', 'order_index']).size()
    position_duplicates = position_duplicates[position_duplicates > 1]
    
    print(f"• (setlist_id, order_index) 중복: {len(position_duplicates)}개")
    
    if len(position_duplicates) > 0:
        print("• 중복 위치들:")
        for (setlist_id, order_idx), count in position_duplicates.head(10).items():
            print(f"  - Setlist {setlist_id}, Position {order_idx}: {count}개")
            
            # 해당 위치의 곡들 보기
            duplicate_songs = df[(df['setlist_id'] == setlist_id) & (df['order_index'] == order_idx)]
            for _, song in duplicate_songs.iterrows():
                print(f"    * {song['song_title']}")
    
    # 3. setlist_id + song_id 중복 (같은 setlist에서 같은 곡이 여러 번)
    print(f"\n🔍 3. setlist_id + song_id 중복 확인 (같은 곡이 setlist에 여러 번):")
    song_duplicates = df.groupby(['setlist_id', 'song_id']).size()
    song_duplicates = song_duplicates[song_duplicates > 1]
    
    print(f"• (setlist_id, song_id) 중복: {len(song_duplicates)}개")
    
    if len(song_duplicates) > 0:
        print("• 중복 곡들:")
        for (setlist_id, song_id), count in song_duplicates.head(10).items():
            song_info = df[(df['setlist_id'] == setlist_id) & (df['song_id'] == song_id)].iloc[0]
            print(f"  - Setlist {setlist_id}: '{song_info['song_title']}' ({count}번)")
            
            # 해당 곡의 모든 위치들 보기
            positions = df[(df['setlist_id'] == setlist_id) & (df['song_id'] == song_id)]['order_index'].tolist()
            print(f"    위치: {positions}")
    
    # 4. setlist_id + song_title 중복 (같은 제목의 곡이 여러 번, 다른 아티스트일 수 있음)
    print(f"\n🔍 4. setlist_id + song_title 중복 확인:")
    title_duplicates = df.groupby(['setlist_id', 'song_title']).size()
    title_duplicates = title_duplicates[title_duplicates > 1]
    
    print(f"• (setlist_id, song_title) 중복: {len(title_duplicates)}개")
    
    if len(title_duplicates) > 0:
        print("• 중복 제목들:")
        for (setlist_id, song_title), count in title_duplicates.head(10).items():
            print(f"  - Setlist {setlist_id}: '{song_title}' ({count}번)")
            
            # 해당 제목의 모든 song_id들 보기 (다른 아티스트인지 확인)
            songs = df[(df['setlist_id'] == setlist_id) & (df['song_title'] == song_title)]
            unique_song_ids = songs['song_id'].unique()
            print(f"    Song IDs: {unique_song_ids} ({'다른 곡' if len(unique_song_ids) > 1 else '같은 곡'})")
    
    # 5. 각 setlist별 중복 통계
    print(f"\n📊 setlist별 중복 현황:")
    
    setlist_stats = []
    for setlist_id in df['setlist_id'].unique():
        setlist_data = df[df['setlist_id'] == setlist_id]
        
        total_songs = len(setlist_data)
        unique_positions = setlist_data['order_index'].nunique()
        unique_song_ids = setlist_data['song_id'].nunique()
        unique_song_titles = setlist_data['song_title'].nunique()
        
        position_dups = total_songs - unique_positions
        song_id_dups = total_songs - unique_song_ids
        title_dups = total_songs - unique_song_titles
        
        if position_dups > 0 or song_id_dups > 0:
            setlist_stats.append({
                'setlist_id': setlist_id,
                'total': total_songs,
                'position_dups': position_dups,
                'song_id_dups': song_id_dups,
                'title_dups': title_dups
            })
    
    if setlist_stats:
        print("• 문제가 있는 setlist들:")
        setlist_stats.sort(key=lambda x: x['position_dups'] + x['song_id_dups'], reverse=True)
        
        for stats in setlist_stats[:10]:
            print(f"  - Setlist {stats['setlist_id']}: {stats['total']}곡")
            if stats['position_dups'] > 0:
                print(f"    * 위치 중복: {stats['position_dups']}개")
            if stats['song_id_dups'] > 0:
                print(f"    * 곡 중복: {stats['song_id_dups']}개")
    else:
        print("• 모든 setlist가 정상입니다!")
    
    # 6. 중복 제거 방안 제안
    print(f"\n💡 중복 제거 방안:")
    
    total_issues = full_duplicates + len(position_duplicates) + len(song_duplicates)
    
    if total_issues > 0:
        print(f"• 발견된 총 중복 문제: {total_issues}개")
        print("• 제거 방법:")
        
        if full_duplicates > 0:
            print(f"  1. 완전 중복 {full_duplicates}개 제거")
        
        if len(position_duplicates) > 0:
            print(f"  2. 위치 중복 {len(position_duplicates)}개 해결:")
            print("     - 같은 위치에 여러 곡이 있는 경우 첫 번째만 유지")
            print("     - 나머지 곡들은 뒤쪽 위치로 재배치")
        
        if len(song_duplicates) > 0:
            print(f"  3. 곡 중복 {len(song_duplicates)}개 해결:")
            print("     - 같은 setlist에서 중복된 곡은 첫 번째만 유지")
        
        return True  # 중복이 있음
    else:
        print("• 중복 문제가 발견되지 않았습니다!")
        return False  # 중복 없음

def remove_setlist_songs_duplicates():
    """setlist_songs.csv의 중복 제거"""
    
    print("\n🧹 setlist_songs.csv 중복 제거 시작")
    print("=" * 60)
    
    # setlist_songs.csv 파일 읽기
    setlist_songs_path = '/Users/youz2me/Xcode/Livith-Data/output/cleaned_data/setlist_songs.csv'
    df = pd.read_csv(setlist_songs_path)
    
    print(f"• 원본 레코드 수: {len(df)}개")
    
    # 1. 완전 중복 제거
    before_count = len(df)
    df = df.drop_duplicates()
    full_dup_removed = before_count - len(df)
    print(f"• 완전 중복 제거: {full_dup_removed}개")
    
    # 2. setlist별로 처리하여 위치/곡 중복 해결
    cleaned_data = []
    
    for setlist_id in df['setlist_id'].unique():
        setlist_data = df[df['setlist_id'] == setlist_id].copy()
        
        # order_index 기준으로 정렬
        setlist_data = setlist_data.sort_values('order_index')
        
        # 중복 제거: setlist_id + song_id 조합으로 첫 번째만 유지
        setlist_cleaned = setlist_data.drop_duplicates(subset=['setlist_id', 'song_id'], keep='first')
        
        # order_index 재정렬 (1부터 순차)
        setlist_cleaned = setlist_cleaned.sort_values('order_index').reset_index(drop=True)
        setlist_cleaned['order_index'] = range(1, len(setlist_cleaned) + 1)
        
        cleaned_data.append(setlist_cleaned)
    
    # 모든 setlist 합치기
    df_cleaned = pd.concat(cleaned_data, ignore_index=True)
    
    removed_count = len(df) - len(df_cleaned)
    print(f"• 위치/곡 중복 제거: {removed_count}개")
    print(f"• 최종 레코드 수: {len(df_cleaned)}개")
    
    # 파일 저장
    df_cleaned.to_csv(setlist_songs_path, index=False, encoding='utf-8')
    print(f"• setlist_songs.csv 저장 완료")
    
    # 결과 검증
    print(f"\n✅ 중복 제거 결과 검증:")
    
    # 위치 중복 재확인
    position_duplicates = df_cleaned.groupby(['setlist_id', 'order_index']).size()
    position_duplicates = position_duplicates[position_duplicates > 1]
    print(f"• 남은 위치 중복: {len(position_duplicates)}개")
    
    # 곡 중복 재확인
    song_duplicates = df_cleaned.groupby(['setlist_id', 'song_id']).size()
    song_duplicates = song_duplicates[song_duplicates > 1]
    print(f"• 남은 곡 중복: {len(song_duplicates)}개")
    
    return df_cleaned

if __name__ == "__main__":
    try:
        # 1. 중복 확인
        has_duplicates = check_setlist_songs_duplicates()
        
        # 2. 중복이 있으면 제거
        if has_duplicates:
            remove_setlist_songs_duplicates()
            
            # 3. 제거 후 재확인
            print("\n" + "="*40)
            print("🔄 중복 제거 후 재확인")
            print("="*40)
            check_setlist_songs_duplicates()
        
        print("\n" + "=" * 60)
        print("🎉 setlist_songs.csv 중복 확인/제거 완료!")
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()