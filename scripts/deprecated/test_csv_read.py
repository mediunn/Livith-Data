#!/usr/bin/env python3
"""
CSV 읽기 인코딩 테스트
"""
import csv
import pandas as pd

def test_csv_reading():
    """CSV 파일 읽기 테스트"""
    csv_path = "/Users/youz2me/Xcode/Livith-Data/output/songs.csv"
    
    print("=" * 60)
    print("CSV 읽기 테스트")
    print("=" * 60)
    
    # 방법 1: 기본 CSV 모듈
    print("\n1. Python CSV 모듈로 읽기:")
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i == 0:  # 첫 번째 행만
                    print(f"   - Title: {row.get('title', '')}")
                    print(f"   - Artist: {row.get('artist', '')}")
                    lyrics = row.get('lyrics', '')
                    if lyrics:
                        print(f"   - Lyrics (처음 100자): {lyrics[:100]}")
                    break
    except Exception as e:
        print(f"   ❌ 오류: {e}")
    
    # 방법 2: UTF-8-sig 인코딩
    print("\n2. UTF-8-sig 인코딩으로 읽기:")
    try:
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i == 0:  # 첫 번째 행만
                    print(f"   - Title: {row.get('title', '')}")
                    print(f"   - Artist: {row.get('artist', '')}")
                    lyrics = row.get('lyrics', '')
                    if lyrics:
                        print(f"   - Lyrics (처음 100자): {lyrics[:100]}")
                    break
    except Exception as e:
        print(f"   ❌ 오류: {e}")
    
    # 방법 3: Pandas로 읽기
    print("\n3. Pandas로 읽기:")
    try:
        df = pd.read_csv(csv_path, encoding='utf-8')
        if not df.empty:
            first_row = df.iloc[0]
            print(f"   - Title: {first_row.get('title', '')}")
            print(f"   - Artist: {first_row.get('artist', '')}")
            lyrics = first_row.get('lyrics', '')
            if pd.notna(lyrics):
                print(f"   - Lyrics (처음 100자): {str(lyrics)[:100]}")
    except Exception as e:
        print(f"   ❌ 오류: {e}")
    
    # 방법 4: 직접 파일 읽기
    print("\n4. 직접 파일 읽기 (처음 500자):")
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            content = f.read(500)
            print(content)
    except Exception as e:
        print(f"   ❌ 오류: {e}")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    test_csv_reading()