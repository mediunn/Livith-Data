#!/usr/bin/env python3
"""
concert_setlists.csv 파일의 빠진 데이터를 채우고 type을 수정하는 스크립트
"""
import pandas as pd
from datetime import datetime

def update_concert_setlists():
    """concert_setlists.csv 파일 업데이트"""
    
    # CSV 파일들 읽기
    print("📁 CSV 파일들 로딩 중...")
    
    # concert_setlists.csv 읽기
    concert_setlists_df = pd.read_csv('/Users/youz2me/Xcode/Livith-Data/output/cleaned_data/concert_setlists.csv')
    
    # concerts.csv 읽기 (concert_id -> concert_title 매핑)
    concerts_df = pd.read_csv('/Users/youz2me/Xcode/Livith-Data/output/cleaned_data/concerts.csv')
    concert_mapping = dict(zip(concerts_df['id'], concerts_df['title']))
    concert_status_mapping = dict(zip(concerts_df['id'], concerts_df['status']))
    
    # setlists.csv 읽기 (setlist_id -> setlist_title 매핑)
    setlists_df = pd.read_csv('/Users/youz2me/Xcode/Livith-Data/output/cleaned_data/setlists.csv')
    setlist_mapping = dict(zip(setlists_df['id'], setlists_df['title']))
    
    print(f"• concert_setlists 레코드: {len(concert_setlists_df)}개")
    print(f"• concerts 매핑: {len(concert_mapping)}개")
    print(f"• setlists 매핑: {len(setlist_mapping)}개")
    
    # 중복 제거 (concert_id + setlist_id 조합으로)
    print("\n🧹 중복 레코드 제거 중...")
    before_count = len(concert_setlists_df)
    concert_setlists_df = concert_setlists_df.drop_duplicates(subset=['concert_id', 'setlist_id'], keep='first')
    after_count = len(concert_setlists_df)
    print(f"• 중복 제거: {before_count}개 → {after_count}개 ({before_count - after_count}개 제거)")
    
    # 빠진 데이터 채우기
    print("\n📝 빠진 데이터 채우기 중...")
    
    updated_count = 0
    type_updated_count = 0
    
    for idx, row in concert_setlists_df.iterrows():
        concert_id = row['concert_id']
        setlist_id = row['setlist_id']
        
        # concert_title 채우기
        if pd.isna(row['concert_title']) or row['concert_title'] == '':
            if concert_id in concert_mapping:
                concert_setlists_df.at[idx, 'concert_title'] = concert_mapping[concert_id]
                updated_count += 1
        
        # setlist_title 채우기
        if pd.isna(row['setlist_title']) or row['setlist_title'] == '':
            if setlist_id in setlist_mapping:
                concert_setlists_df.at[idx, 'setlist_title'] = setlist_mapping[setlist_id]
                updated_count += 1
        
        # type 업데이트 (콘서트 상태에 따라)
        if concert_id in concert_status_mapping:
            concert_status = concert_status_mapping[concert_id]
            
            # 콘서트 상태에 따른 type 매핑
            if concert_status == 'COMPLETED':
                new_type = 'PAST'
            elif concert_status == 'UPCOMING':
                new_type = 'EXPECTED'
            else:  # ONGOING
                new_type = 'ONGOING'
            
            # type이 다르면 업데이트
            if row['type'] != new_type:
                concert_setlists_df.at[idx, 'type'] = new_type
                type_updated_count += 1
    
    print(f"• 제목 데이터 채움: {updated_count}개 필드")
    print(f"• type 업데이트: {type_updated_count}개 레코드")
    
    # 결과 확인
    print("\n📊 업데이트 결과 확인:")
    
    # type별 분포
    type_counts = concert_setlists_df['type'].value_counts()
    print("• type 분포:")
    for type_name, count in type_counts.items():
        print(f"  - {type_name}: {count}개")
    
    # 빈 값 확인
    empty_concert_titles = concert_setlists_df['concert_title'].isna().sum() + (concert_setlists_df['concert_title'] == '').sum()
    empty_setlist_titles = concert_setlists_df['setlist_title'].isna().sum() + (concert_setlists_df['setlist_title'] == '').sum()
    
    print(f"• 빈 concert_title: {empty_concert_titles}개")
    print(f"• 빈 setlist_title: {empty_setlist_titles}개")
    
    # 업데이트된 파일 저장
    output_path = '/Users/youz2me/Xcode/Livith-Data/output/cleaned_data/concert_setlists.csv'
    concert_setlists_df.to_csv(output_path, index=False, encoding='utf-8')
    
    print(f"\n✅ 업데이트된 파일 저장 완료: {output_path}")
    
    # 샘플 데이터 출력
    print("\n📋 업데이트된 데이터 샘플 (상위 5개):")
    sample_columns = ['concert_id', 'setlist_id', 'type', 'concert_title', 'setlist_title']
    for i, (_, row) in enumerate(concert_setlists_df.head().iterrows()):
        print(f"\n{i+1}.")
        for col in sample_columns:
            value = row[col]
            if isinstance(value, str) and len(value) > 50:
                value = value[:47] + "..."
            print(f"  • {col}: {value}")

if __name__ == "__main__":
    print("🔄 concert_setlists.csv 업데이트 시작")
    print("=" * 60)
    
    try:
        update_concert_setlists()
        print("\n" + "=" * 60)
        print("🎉 모든 업데이트 작업 완료!")
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()