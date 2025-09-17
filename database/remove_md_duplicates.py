#!/usr/bin/env python3
"""
md.csv에서 concert_id와 name 조합으로 중복 제거 스크립트
"""
import pandas as pd

def remove_md_duplicates():
    """md.csv에서 concert_id + name 조합으로 중복 제거"""
    
    print("🔄 md.csv 중복 제거 시작")
    print("=" * 60)
    
    # md.csv 파일 읽기
    md_path = '/Users/youz2me/Xcode/Livith-Data/output/cleaned_data/md.csv'
    
    print("📁 md.csv 파일 로딩 중...")
    md_df = pd.read_csv(md_path)
    
    print(f"• 원본 레코드 수: {len(md_df)}개")
    
    # concert_id + name 조합별 중복 확인
    print("\n🔍 중복 분석 중...")
    
    # 중복 확인
    duplicate_combinations = md_df.groupby(['concert_id', 'name']).size()
    duplicated_items = duplicate_combinations[duplicate_combinations > 1]
    
    print(f"• 중복된 (concert_id, name) 조합: {len(duplicated_items)}개")
    
    if len(duplicated_items) > 0:
        print("\n• 중복 항목들:")
        for (concert_id, name), count in duplicated_items.head(10).items():
            name_display = name[:50] + "..." if len(name) > 50 else name
            print(f"  - Concert {concert_id}: '{name_display}' ({count}개)")
    
    # 중복 제거 (concert_id + name 조합으로 첫 번째 것만 유지)
    print(f"\n🧹 중복 제거 중...")
    
    md_df_cleaned = md_df.drop_duplicates(
        subset=['concert_id', 'name'], 
        keep='first'  # 첫 번째 레코드 유지
    ).reset_index(drop=True)
    
    removed_count = len(md_df) - len(md_df_cleaned)
    print(f"• 중복 제거 후 레코드 수: {len(md_df_cleaned)}개")
    print(f"• 제거된 레코드 수: {removed_count}개")
    
    # 콘서트별 MD 분포 확인
    print(f"\n📊 콘서트별 MD 분포:")
    
    # 콘서트 정보 매핑 (concerts.csv에서)
    try:
        concerts_df = pd.read_csv('/Users/youz2me/Xcode/Livith-Data/output/cleaned_data/concerts.csv')
        concert_mapping = dict(zip(concerts_df['id'], concerts_df['title']))
        
        concert_md_counts = md_df_cleaned.groupby('concert_id').size().sort_values(ascending=False)
        
        print("• 상위 10개 콘서트별 MD 수:")
        for concert_id, count in concert_md_counts.head(10).items():
            concert_title = concert_mapping.get(concert_id, f"Concert {concert_id}")
            title_display = concert_title[:40] + "..." if len(concert_title) > 40 else concert_title
            print(f"  - {title_display}: {count}개")
            
    except Exception as e:
        print(f"  ⚠️ 콘서트 정보 매핑 실패: {e}")
        
        # 콘서트 ID별 분포만 표시
        concert_counts = md_df_cleaned['concert_id'].value_counts().head(10)
        print("• 상위 10개 콘서트 ID별 MD 수:")
        for concert_id, count in concert_counts.items():
            print(f"  - Concert {concert_id}: {count}개")
    
    # 파일 저장
    print(f"\n💾 정리된 파일 저장 중...")
    md_df_cleaned.to_csv(md_path, index=False, encoding='utf-8')
    print(f"• md.csv 저장 완료: {len(md_df_cleaned)}개 레코드")
    
    # 샘플 데이터 출력
    print(f"\n📋 정리된 데이터 샘플 (상위 5개):")
    sample_columns = ['concert_id', 'name', 'price']
    for i, (_, row) in enumerate(md_df_cleaned.head().iterrows()):
        print(f"\n{i+1}.")
        for col in sample_columns:
            value = row[col]
            if col == 'name' and isinstance(value, str) and len(value) > 40:
                value = value[:37] + "..."
            print(f"  • {col}: {value}")
    
    # 가격대별 분포
    print(f"\n💰 가격대별 MD 분포:")
    try:
        # price 컬럼을 숫자로 변환 (빈 값은 0으로)
        md_df_cleaned['price_numeric'] = pd.to_numeric(md_df_cleaned['price'], errors='coerce').fillna(0)
        
        price_ranges = [
            (0, 0, "무료"),
            (1, 10000, "1만원 미만"),
            (10000, 30000, "1-3만원"),
            (30000, 50000, "3-5만원"),
            (50000, 100000, "5-10만원"),
            (100000, float('inf'), "10만원 이상")
        ]
        
        for min_price, max_price, label in price_ranges:
            if max_price == float('inf'):
                count = len(md_df_cleaned[md_df_cleaned['price_numeric'] >= min_price])
            else:
                count = len(md_df_cleaned[
                    (md_df_cleaned['price_numeric'] >= min_price) & 
                    (md_df_cleaned['price_numeric'] < max_price)
                ])
            print(f"  • {label}: {count}개")
            
    except Exception as e:
        print(f"  ⚠️ 가격대 분석 실패: {e}")

if __name__ == "__main__":
    try:
        remove_md_duplicates()
        print("\n" + "=" * 60)
        print("🎉 md.csv 중복 제거 작업 완료!")
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()