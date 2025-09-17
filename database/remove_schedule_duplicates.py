#!/usr/bin/env python3
"""
schedule.csv에서 concert_id와 category 조합으로 중복 제거 스크립트
"""
import pandas as pd

def remove_schedule_duplicates():
    """schedule.csv에서 concert_id + category 조합으로 중복 제거"""
    
    print("🔄 schedule.csv 중복 제거 시작")
    print("=" * 60)
    
    # schedule.csv 파일 읽기
    schedule_path = '/Users/youz2me/Xcode/Livith-Data/output/cleaned_data/schedule.csv'
    
    print("📁 schedule.csv 파일 로딩 중...")
    schedule_df = pd.read_csv(schedule_path)
    
    print(f"• 원본 레코드 수: {len(schedule_df)}개")
    
    # concert_id + category 조합별 중복 확인
    print("\n🔍 중복 분석 중...")
    
    # 중복 확인
    duplicate_combinations = schedule_df.groupby(['concert_id', 'category']).size()
    duplicated_items = duplicate_combinations[duplicate_combinations > 1]
    
    print(f"• 중복된 (concert_id, category) 조합: {len(duplicated_items)}개")
    
    if len(duplicated_items) > 0:
        print("\n• 중복 항목들:")
        for (concert_id, category), count in duplicated_items.head(10).items():
            print(f"  - Concert {concert_id}: '{category}' ({count}개)")
    
    # 중복 제거 (concert_id + category 조합으로 첫 번째 것만 유지)
    print(f"\n🧹 중복 제거 중...")
    
    schedule_df_cleaned = schedule_df.drop_duplicates(
        subset=['concert_id', 'category'], 
        keep='first'  # 첫 번째 레코드 유지
    ).reset_index(drop=True)
    
    removed_count = len(schedule_df) - len(schedule_df_cleaned)
    print(f"• 중복 제거 후 레코드 수: {len(schedule_df_cleaned)}개")
    print(f"• 제거된 레코드 수: {removed_count}개")
    
    # 카테고리별 분포 확인
    print(f"\n📊 카테고리별 스케줄 분포:")
    category_counts = schedule_df_cleaned['category'].value_counts()
    for category, count in category_counts.items():
        print(f"  • {category}: {count}개")
    
    # type별 분포 확인
    print(f"\n📊 type별 스케줄 분포:")
    type_counts = schedule_df_cleaned['type'].value_counts()
    for type_name, count in type_counts.items():
        print(f"  • {type_name}: {count}개")
    
    # 콘서트별 스케줄 분포 확인
    print(f"\n📊 콘서트별 스케줄 분포:")
    
    # 콘서트 정보 매핑 (concerts.csv에서)
    try:
        concerts_df = pd.read_csv('/Users/youz2me/Xcode/Livith-Data/output/cleaned_data/concerts.csv')
        concert_mapping = dict(zip(concerts_df['id'], concerts_df['title']))
        
        concert_schedule_counts = schedule_df_cleaned.groupby('concert_id').size().sort_values(ascending=False)
        
        print("• 상위 10개 콘서트별 스케줄 수:")
        for concert_id, count in concert_schedule_counts.head(10).items():
            concert_title = concert_mapping.get(concert_id, f"Concert {concert_id}")
            title_display = concert_title[:40] + "..." if len(concert_title) > 40 else concert_title
            print(f"  - {title_display}: {count}개")
            
    except Exception as e:
        print(f"  ⚠️ 콘서트 정보 매핑 실패: {e}")
        
        # 콘서트 ID별 분포만 표시
        concert_counts = schedule_df_cleaned['concert_id'].value_counts().head(10)
        print("• 상위 10개 콘서트 ID별 스케줄 수:")
        for concert_id, count in concert_counts.items():
            print(f"  - Concert {concert_id}: {count}개")
    
    # scheduled_at이 있는 레코드와 없는 레코드 분포
    print(f"\n📊 scheduled_at 데이터 분포:")
    has_scheduled_at = schedule_df_cleaned['scheduled_at'].notna().sum()
    no_scheduled_at = len(schedule_df_cleaned) - has_scheduled_at
    print(f"  • scheduled_at 있음: {has_scheduled_at}개")
    print(f"  • scheduled_at 없음: {no_scheduled_at}개")
    
    # 파일 저장
    print(f"\n💾 정리된 파일 저장 중...")
    schedule_df_cleaned.to_csv(schedule_path, index=False, encoding='utf-8')
    print(f"• schedule.csv 저장 완료: {len(schedule_df_cleaned)}개 레코드")
    
    # 샘플 데이터 출력
    print(f"\n📋 정리된 데이터 샘플 (상위 5개):")
    sample_columns = ['concert_id', 'category', 'scheduled_at', 'type']
    for i, (_, row) in enumerate(schedule_df_cleaned.head().iterrows()):
        print(f"\n{i+1}.")
        for col in sample_columns:
            value = row[col]
            if pd.isna(value):
                value = "없음"
            elif col == 'scheduled_at' and isinstance(value, str):
                # 날짜 포맷 간략화
                if len(str(value)) > 19:
                    value = str(value)[:19]
            print(f"  • {col}: {value}")

if __name__ == "__main__":
    try:
        remove_schedule_duplicates()
        print("\n" + "=" * 60)
        print("🎉 schedule.csv 중복 제거 작업 완료!")
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()