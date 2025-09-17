#!/usr/bin/env python3
"""
home_concert_sections와 search_concert_sections에서 concert_title 기준으로 중복 제거 스크립트
"""
import pandas as pd

def remove_duplicates_from_sections():
    """home_concert_sections와 search_concert_sections에서 중복 제거"""
    
    print("🔄 섹션 파일들의 중복 제거 시작")
    print("=" * 60)
    
    # 1. home_concert_sections 처리
    print("\n📁 home_concert_sections.csv 처리 중...")
    
    home_sections_path = '/Users/youz2me/Xcode/Livith-Data/output/cleaned_data/home_concert_sections.csv'
    home_df = pd.read_csv(home_sections_path)
    
    print(f"• 원본 레코드 수: {len(home_df)}개")
    
    # concert_title 기준 중복 확인
    duplicates_home = home_df.groupby('concert_title').size()
    duplicated_titles_home = duplicates_home[duplicates_home > 1]
    
    print(f"• 중복된 concert_title: {len(duplicated_titles_home)}개")
    for title, count in duplicated_titles_home.head().items():
        print(f"  - {title[:50]}{'...' if len(title) > 50 else ''}: {count}개")
    
    # 중복 제거 (section_title, concert_title, sorted_index 조합으로 고유성 보장)
    # 각 섹션별로 같은 콘서트는 한 번만 나타나도록 처리
    home_df_cleaned = home_df.drop_duplicates(
        subset=['home_section_id', 'concert_id', 'concert_title'], 
        keep='first'
    ).reset_index(drop=True)
    
    print(f"• 중복 제거 후 레코드 수: {len(home_df_cleaned)}개")
    print(f"• 제거된 레코드 수: {len(home_df) - len(home_df_cleaned)}개")
    
    # 2. search_concert_sections 처리
    print("\n📁 search_concert_sections.csv 처리 중...")
    
    search_sections_path = '/Users/youz2me/Xcode/Livith-Data/output/cleaned_data/search_concert_sections.csv'
    search_df = pd.read_csv(search_sections_path)
    
    print(f"• 원본 레코드 수: {len(search_df)}개")
    
    # concert_title 기준 중복 확인
    duplicates_search = search_df.groupby('concert_title').size()
    duplicated_titles_search = duplicates_search[duplicates_search > 1]
    
    print(f"• 중복된 concert_title: {len(duplicated_titles_search)}개")
    for title, count in duplicated_titles_search.head().items():
        print(f"  - {title[:50]}{'...' if len(title) > 50 else ''}: {count}개")
    
    # 중복 제거 (section_title, concert_title, sorted_index 조합으로 고유성 보장)
    search_df_cleaned = search_df.drop_duplicates(
        subset=['search_section_id', 'concert_id', 'concert_title'], 
        keep='first'
    ).reset_index(drop=True)
    
    print(f"• 중복 제거 후 레코드 수: {len(search_df_cleaned)}개")
    print(f"• 제거된 레코드 수: {len(search_df) - len(search_df_cleaned)}개")
    
    # 3. sorted_index 재정렬
    print("\n🔄 sorted_index 재정렬 중...")
    
    # home_concert_sections의 sorted_index 재정렬
    for section_id in home_df_cleaned['home_section_id'].unique():
        section_mask = home_df_cleaned['home_section_id'] == section_id
        section_data = home_df_cleaned[section_mask].copy()
        section_data = section_data.sort_values(['concert_id']).reset_index(drop=True)
        
        # sorted_index를 1부터 순서대로 재할당
        new_indices = range(1, len(section_data) + 1)
        home_df_cleaned.loc[section_mask, 'sorted_index'] = new_indices
    
    # search_concert_sections의 sorted_index 재정렬  
    for section_id in search_df_cleaned['search_section_id'].unique():
        section_mask = search_df_cleaned['search_section_id'] == section_id
        section_data = search_df_cleaned[section_mask].copy()
        section_data = section_data.sort_values(['concert_id']).reset_index(drop=True)
        
        # sorted_index를 1부터 순서대로 재할당
        new_indices = range(1, len(section_data) + 1)
        search_df_cleaned.loc[section_mask, 'sorted_index'] = new_indices
    
    # 4. 파일 저장
    print("\n💾 업데이트된 파일들 저장 중...")
    
    home_df_cleaned.to_csv(home_sections_path, index=False, encoding='utf-8')
    print(f"• home_concert_sections.csv 저장 완료: {len(home_df_cleaned)}개 레코드")
    
    search_df_cleaned.to_csv(search_sections_path, index=False, encoding='utf-8')
    print(f"• search_concert_sections.csv 저장 완료: {len(search_df_cleaned)}개 레코드")
    
    # 5. 결과 요약
    print("\n📊 최종 결과 요약:")
    
    # home_concert_sections 섹션별 분포
    print("\n• home_concert_sections 섹션별 콘서트 수:")
    home_section_counts = home_df_cleaned.groupby(['home_section_id', 'section_title']).size()
    for (section_id, section_title), count in home_section_counts.items():
        print(f"  - {section_title}: {count}개 콘서트")
    
    # search_concert_sections 섹션별 분포
    print("\n• search_concert_sections 섹션별 콘서트 수:")
    search_section_counts = search_df_cleaned.groupby(['search_section_id', 'section_title']).size()
    for (section_id, section_title), count in search_section_counts.items():
        print(f"  - {section_title}: {count}개 콘서트")
    
    # 6. 샘플 데이터 출력
    print("\n📋 home_concert_sections 샘플 (상위 5개):")
    sample_columns = ['home_section_id', 'section_title', 'concert_title', 'sorted_index']
    for i, (_, row) in enumerate(home_df_cleaned.head().iterrows()):
        print(f"\n{i+1}.")
        for col in sample_columns:
            value = row[col]
            if isinstance(value, str) and len(value) > 40:
                value = value[:37] + "..."
            print(f"  • {col}: {value}")
    
    print("\n📋 search_concert_sections 샘플 (상위 5개):")
    sample_columns = ['search_section_id', 'section_title', 'concert_title', 'sorted_index']
    for i, (_, row) in enumerate(search_df_cleaned.head().iterrows()):
        print(f"\n{i+1}.")
        for col in sample_columns:
            value = row[col]
            if isinstance(value, str) and len(value) > 40:
                value = value[:37] + "..."
            print(f"  • {col}: {value}")

if __name__ == "__main__":
    try:
        remove_duplicates_from_sections()
        print("\n" + "=" * 60)
        print("🎉 모든 중복 제거 작업 완료!")
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()