#!/usr/bin/env python3
"""
setlist_songs.csv의 order_index를 각 setlist별로 1부터 시작하도록 수정하는 스크립트
"""
import pandas as pd

def fix_setlist_songs_order_index():
    """setlist_songs.csv의 order_index를 setlist별로 수정"""
    
    print("🔄 setlist_songs.csv order_index 수정 시작")
    print("=" * 60)
    
    # setlist_songs.csv 파일 읽기
    setlist_songs_path = '/Users/youz2me/Xcode/Livith-Data/output/cleaned_data/setlist_songs.csv'
    
    print("📁 setlist_songs.csv 파일 로딩 중...")
    df = pd.read_csv(setlist_songs_path)
    
    print(f"• 원본 레코드 수: {len(df)}개")
    
    # 현재 order_index 분포 확인
    print(f"\n🔍 현재 order_index 분포:")
    print(f"• 최소값: {df['order_index'].min()}")
    print(f"• 최대값: {df['order_index'].max()}")
    
    # setlist별 곡 수 확인
    setlist_song_counts = df.groupby('setlist_id').size().sort_values(ascending=False)
    print(f"\n📊 setlist별 곡 수 (상위 10개):")
    for setlist_id, count in setlist_song_counts.head(10).items():
        print(f"  • Setlist {setlist_id}: {count}곡")
    
    # 현재 order_index가 잘못된 setlist 확인
    print(f"\n🔍 order_index 문제 확인:")
    problem_setlists = []
    
    for setlist_id in df['setlist_id'].unique():
        setlist_data = df[df['setlist_id'] == setlist_id].sort_values('order_index')
        min_order = setlist_data['order_index'].min()
        max_order = setlist_data['order_index'].max()
        expected_max = len(setlist_data)
        
        if min_order != 1 or max_order != expected_max:
            problem_setlists.append((setlist_id, min_order, max_order, expected_max))
    
    print(f"• 문제가 있는 setlist: {len(problem_setlists)}개")
    for setlist_id, min_val, max_val, expected in problem_setlists[:5]:
        print(f"  - Setlist {setlist_id}: {min_val}-{max_val} (기대값: 1-{expected})")
    
    # order_index 수정
    print(f"\n🔧 order_index 수정 중...")
    
    # 각 setlist별로 order_index를 1부터 순차적으로 재할당
    df_fixed = df.copy()
    
    for setlist_id in df_fixed['setlist_id'].unique():
        # 해당 setlist의 데이터를 현재 order_index 순서대로 정렬
        setlist_mask = df_fixed['setlist_id'] == setlist_id
        setlist_data = df_fixed[setlist_mask].sort_values('order_index').copy()
        
        # 1부터 순차적으로 새로운 order_index 할당
        new_order_indices = range(1, len(setlist_data) + 1)
        
        # 원본 데이터프레임의 해당 위치에 새로운 order_index 값 할당
        df_fixed.loc[setlist_data.index, 'order_index'] = list(new_order_indices)
    
    # 수정 결과 확인
    print(f"\n📊 수정 후 order_index 분포:")
    print(f"• 최소값: {df_fixed['order_index'].min()}")
    print(f"• 최대값: {df_fixed['order_index'].max()}")
    
    # 수정 후 문제 확인
    fixed_problem_setlists = []
    for setlist_id in df_fixed['setlist_id'].unique():
        setlist_data = df_fixed[df_fixed['setlist_id'] == setlist_id].sort_values('order_index')
        min_order = setlist_data['order_index'].min()
        max_order = setlist_data['order_index'].max()
        expected_max = len(setlist_data)
        
        if min_order != 1 or max_order != expected_max:
            fixed_problem_setlists.append((setlist_id, min_order, max_order, expected_max))
    
    print(f"• 수정 후 문제가 있는 setlist: {len(fixed_problem_setlists)}개")
    
    # 파일 저장
    print(f"\n💾 수정된 파일 저장 중...")
    df_fixed.to_csv(setlist_songs_path, index=False, encoding='utf-8')
    print(f"• setlist_songs.csv 저장 완료: {len(df_fixed)}개 레코드")
    
    # 샘플 데이터 출력 - 여러 setlist의 첫 번째 곡들 확인
    print(f"\n📋 수정된 데이터 샘플 (각 setlist의 첫 번째 곡들):")
    sample_columns = ['setlist_id', 'order_index', 'song_title']
    
    # 각 setlist의 첫 번째 곡들 (order_index = 1)
    first_songs = df_fixed[df_fixed['order_index'] == 1].head(5)
    
    for i, (_, row) in enumerate(first_songs.iterrows()):
        print(f"\n{i+1}.")
        for col in sample_columns:
            value = row[col]
            if col == 'song_title' and isinstance(value, str) and len(value) > 40:
                value = value[:37] + "..."
            print(f"  • {col}: {value}")
    
    # 특정 setlist의 전체 곡 순서 확인 (첫 번째 setlist)
    if not df_fixed.empty:
        first_setlist_id = df_fixed['setlist_id'].iloc[0]
        first_setlist_songs = df_fixed[df_fixed['setlist_id'] == first_setlist_id].sort_values('order_index')
        
        print(f"\n📋 Setlist {first_setlist_id} 전체 곡 순서:")
        for _, song in first_setlist_songs.iterrows():
            song_title = song['song_title']
            if len(song_title) > 30:
                song_title = song_title[:27] + "..."
            print(f"  {song['order_index']:2d}. {song_title}")

if __name__ == "__main__":
    try:
        fix_setlist_songs_order_index()
        print("\n" + "=" * 60)
        print("🎉 setlist_songs order_index 수정 작업 완료!")
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()