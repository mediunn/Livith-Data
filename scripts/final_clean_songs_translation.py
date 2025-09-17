#!/usr/bin/env python3
"""
songs.csv의 translation 필드 최종 정리 스크립트
- pronunciation 필드에서 발음 추출
- 콘서트 정보나 부가 설명 제거
- 발음만 있고 해석이 없는 경우 빈 값으로 처리
"""
import pandas as pd
import re
from pathlib import Path

def extract_clean_pronunciation(text):
    """
    텍스트에서 순수 발음만 추출
    """
    if pd.isna(text) or not text:
        return ''
    
    text = str(text).strip()
    lines = text.split('\n')
    clean_lines = []
    
    for line in lines:
        line = line.strip()
        
        # 빈 줄은 유지
        if not line:
            clean_lines.append('')
            continue
        
        # 콘서트 정보나 메타 정보 라인 제거
        if any(keyword in line for keyword in [
            '**', '콘서트', '내한', '공연', '티켓', '판매', '발표',
            'Google Search', '검색', '기준', '정보', '확인', '예정'
        ]):
            continue
        
        # 한글 발음인지 확인 (영어를 한글로 표기한 것)
        if re.search(r'[가-힣]', line):
            # 자연스러운 한국어 문장이 아닌 발음 표기인지 확인
            if is_pronunciation_line(line):
                clean_lines.append(line)
    
    # 연속된 빈 줄 정리
    result = []
    prev_empty = False
    for line in clean_lines:
        if not line:
            if not prev_empty and result:  # 중간에 빈 줄만 허용
                result.append('')
            prev_empty = True
        else:
            result.append(line)
            prev_empty = False
    
    # 앞뒤 빈 줄 제거
    while result and not result[0]:
        result.pop(0)
    while result and not result[-1]:
        result.pop()
    
    return '\\n'.join(result) if result else ''

def is_pronunciation_line(line):
    """
    발음 표기 라인인지 판단
    """
    # 영어 발음의 한글 표기 특징
    pronunciation_patterns = [
        r'[가-힣]+[스즈]\\b',  # ~스, ~즈 (복수형)
        r'\\b[아에이오우][가-힣]+',  # 영어 발음의 모음 시작
        r'[가-힣]*[링닝밍싱]\\b',  # ~ing 발음
        r'[가-힣]*[션천]\\b',  # ~tion 발음
        r'더\\s+[가-힣]+',  # the + 단어
        r'앤\\s+[가-힣]+',  # and + 단어
        r'인\\s+더',  # in the
        r'오브\\s+더',  # of the
    ]
    
    for pattern in pronunciation_patterns:
        if re.search(pattern, line):
            return True
    
    # 짧은 단어들로 이루어진 발음 표기
    words = line.split()
    if len(words) >= 3:
        short_word_count = sum(1 for word in words if len(word) <= 4 and re.match(r'^[가-힣]+$', word))
        if short_word_count >= len(words) * 0.6:  # 60% 이상이 짧은 한글 단어
            return True
    
    return False

def should_keep_translation(text):
    """
    translation 필드를 유지할지 판단
    발음과 해석이 모두 있어야 유지
    """
    if not text:
        return False
    
    # '발음\n\n해석' 형식인지 확인
    parts = text.split('\\n\\n')
    if len(parts) != 2:
        return False
    
    pronunciation_part = parts[0].strip()
    translation_part = parts[1].strip()
    
    # 발음 부분 검증
    if not pronunciation_part or not re.search(r'[가-힣]', pronunciation_part):
        return False
    
    # 해석 부분 검증 (자연스러운 한국어여야 함)
    if not translation_part or not re.search(r'[가-힣]', translation_part):
        return False
    
    # 해석이 발음과 비슷하면 (중복이면) 제거
    if pronunciation_part == translation_part:
        return False
    
    return True

def main():
    """메인 함수"""
    csv_path = Path('/Users/youz2me/Xcode/Livith-Data/output/cleaned_data/songs.csv')
    
    print("=" * 70)
    print("🧹 Songs Translation 필드 최종 정리")
    print("=" * 70)
    print("\\n📌 처리 규칙:")
    print("  1. pronunciation 필드에서 순수 발음만 추출")
    print("  2. 콘서트 정보, 메타데이터 제거")
    print("  3. 발음만 있고 해석 없는 경우 → 빈 값")
    print("  4. '발음\\n\\n해석' 형식만 유지")
    print("  5. 부적절한 내용은 모두 제거")
    print()
    
    print(f"📁 파일 읽기: {csv_path}")
    df = pd.read_csv(csv_path, encoding='utf-8')
    
    print(f"  • 총 레코드: {len(df)}개")
    
    # 백업 생성
    backup_path = csv_path.with_suffix('.csv.final_backup')
    df.to_csv(backup_path, index=False, encoding='utf-8')
    print(f"  • 백업 생성: {backup_path}")
    
    # 현재 상태 확인
    has_pronunciation = (df['pronunciation'].notna() & (df['pronunciation'] != '')).sum()
    has_translation = (df['translation'].notna() & (df['translation'] != '')).sum()
    
    print(f"  • pronunciation 있는 레코드: {has_pronunciation}개")
    print(f"  • translation 있는 레코드: {has_translation}개")
    
    # 처리
    print(f"\\n🧹 필드 정리 중...")
    
    cleaned_count = 0
    removed_count = 0
    
    for idx, row in df.iterrows():
        original_pronunciation = row['pronunciation']
        original_translation = row['translation']
        
        # pronunciation 필드 정리
        if pd.notna(original_pronunciation) and original_pronunciation:
            clean_pronunciation = extract_clean_pronunciation(original_pronunciation)
            
            if clean_pronunciation:
                # 발음만 있는 경우 - 해석이 없으므로 빈 값으로
                df.at[idx, 'translation'] = ''
                df.at[idx, 'pronunciation'] = ''
                print(f"  ⚠️ [{idx}] {row['title']} - 발음만 있음, 제거")
                removed_count += 1
            else:
                df.at[idx, 'pronunciation'] = ''
                removed_count += 1
        
        # translation 필드 검증
        if pd.notna(original_translation) and original_translation:
            if should_keep_translation(str(original_translation)):
                cleaned_count += 1
                print(f"  ✅ [{idx}] {row['title']} - translation 유지")
            else:
                df.at[idx, 'translation'] = ''
                print(f"  ❌ [{idx}] {row['title']} - 부적절한 translation 제거")
                removed_count += 1
    
    print(f"\\n📊 정리 결과:")
    print(f"  • 유지된 translation: {cleaned_count}개")
    print(f"  • 제거/정리된 필드: {removed_count}개")
    
    # 최종 확인
    final_translation_count = (df['translation'].notna() & (df['translation'] != '')).sum()
    final_pronunciation_count = (df['pronunciation'].notna() & (df['pronunciation'] != '')).sum()
    
    print(f"\\n📊 최종 상태:")
    print(f"  • translation 있는 레코드: {final_translation_count}개")
    print(f"  • pronunciation 있는 레코드: {final_pronunciation_count}개")
    
    # 저장
    print(f"\\n💾 저장 중: {csv_path}")
    df.to_csv(csv_path, index=False, encoding='utf-8')
    
    print(f"\\n" + "=" * 70)
    print("✅ Songs Translation 필드 최종 정리 완료!")
    print(f"  • 백업: {backup_path}")
    print(f"  • 최종 유효한 translation: {final_translation_count}개")
    print("=" * 70)
    
    # 현재 데이터 상태 요약
    if final_translation_count == 0:
        print("\\n🔍 현재 상태:")
        print("  • translation 필드가 비어있습니다")
        print("  • AI로 발음과 해석을 생성해야 합니다")
        print("  • 형식: '발음\\n\\n해석'")

if __name__ == "__main__":
    main()