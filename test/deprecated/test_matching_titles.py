#!/usr/bin/env python3
import pandas as pd

# CSV 파일 읽기
setlists_df = pd.read_csv('output/setlists.csv')
concert_setlists_df = pd.read_csv('output/concert_setlists.csv')

print("=" * 60)
print("setlists.csv의 title 필드:")
print("-" * 60)
for title in setlists_df['title'].tolist():
    print(f"  - {title}")

print("\n" + "=" * 60)
print("concert_setlists.csv의 setlist_title 필드:")
print("-" * 60)
for title in concert_setlists_df['setlist_title'].tolist():
    print(f"  - {title}")

print("\n" + "=" * 60)
print("매칭 확인:")
print("-" * 60)

# 각 concert_setlists의 setlist_title이 setlists의 title에 있는지 확인
for cs_title in concert_setlists_df['setlist_title'].tolist():
    if cs_title in setlists_df['title'].tolist():
        print(f"✅ '{cs_title}' - 매칭됨")
    else:
        print(f"❌ '{cs_title}' - setlists.csv에 없음")

print("\n" + "=" * 60)
print("앞으로의 데이터 수집 규칙:")
print("-" * 60)
print("1. setlists.csv의 title:")
print("   - '콘서트명 예상 셋리스트' (예상)")
print("   - '콘서트명 셋리스트' (과거)")
print("\n2. concert_setlists.csv의 setlist_title:")
print("   - setlists.csv의 title과 정확히 동일")
print("\n3. 두 파일이 정확히 매칭되어 연결됨")