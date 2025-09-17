#!/usr/bin/env python3
"""
songs.csv의 translation 필드를 정리하는 스크립트

중요 규칙:
1. translation 필드는 "발음\n\n해석" 형식만 허용
2. 원문(lyrics)은 별도 필드에 있으므로 translation에서 제거
3. 괄호 안의 부가 설명이나 곡 정보 등 모두 제거
4. 줄바꿈은 원문과 동일하게 유지 (원문 3번째 줄 = 발음 3번째 줄 = 해석 3번째 줄)
5. 순수 발음과 순수 해석만 포함
"""

import pandas as pd
import re
from pathlib import Path

def extract_pronunciation_and_translation(text):
    """
    translation 필드에서 발음과 해석 추출
    
    형식:
    - 입력: 다양한 형식의 텍스트
    - 출력: "발음\n\n해석" 형식 또는 빈 문자열
    """
    if pd.isna(text) or text == '':
        return ''
    
    text = str(text).strip()
    
    # 빈 텍스트 처리
    if not text:
        return ''
    
    # \n\n으로 구분된 파트들 확인
    parts = text.split('\n\n')
    
    # Case 1: 이미 올바른 형식 (발음\n\n해석)
    if len(parts) == 2:
        pronunciation = clean_text_content(parts[0])
        translation = clean_text_content(parts[1])
        
        # 둘 다 유효한 경우만 반환
        if is_pronunciation(pronunciation) and is_translation(translation):
            return f"{pronunciation}\n\n{translation}"
    
    # Case 2: 3개 파트 (원문\n\n발음\n\n해석)
    if len(parts) == 3:
        # 원문 제거, 발음과 해석만 유지
        pronunciation = clean_text_content(parts[1])
        translation = clean_text_content(parts[2])
        
        if is_pronunciation(pronunciation) and is_translation(translation):
            return f"{pronunciation}\n\n{translation}"
    
    # Case 3: 단일 텍스트에서 발음과 해석 구분 시도
    if len(parts) == 1:
        lines = text.split('\n')
        
        # 발음과 해석 구분점 찾기
        pronunciation_lines = []
        translation_lines = []
        is_translation_part = False
        
        for line in lines:
            line = line.strip()
            
            # 섹션 마커 감지
            if any(marker in line.lower() for marker in ['translation:', '해석:', '번역:']):
                is_translation_part = True
                continue
            elif any(marker in line.lower() for marker in ['pronunciation:', '발음:']):
                is_translation_part = False
                continue
            
            # 내용 추가
            if is_translation_part:
                translation_lines.append(line)
            else:
                # 한글 발음인지 확인
                if re.search(r'[가-힣]', line) or not line:
                    pronunciation_lines.append(line)
        
        if pronunciation_lines and translation_lines:
            pronunciation = clean_text_content('\n'.join(pronunciation_lines))
            translation = clean_text_content('\n'.join(translation_lines))
            
            if pronunciation and translation:
                return f"{pronunciation}\n\n{translation}"
    
    # 유효한 형식을 찾지 못한 경우
    return ''

def clean_text_content(text):
    """
    텍스트에서 부가 정보 제거하고 순수 내용만 유지
    """
    if not text:
        return ''
    
    lines = text.strip().split('\n')
    cleaned_lines = []
    
    for line in lines:
        # 메타 정보 라인 제거 ([Verse 1], [Chorus] 등)
        if re.match(r'^\[.*\]$', line.strip()):
            continue
        
        # 제목이나 헤더 라인 제거
        if any(keyword in line.lower() for keyword in [
            'lyrics', 'pronunciation', 'translation',
            '가사', '발음', '해석', '번역', '원문'
        ]) and ':' in line:
            continue
        
        # 곡 정보 제거 (아티스트 - 제목 형식)
        if ' - ' in line and line.count('-') == 1:
            # 실제 가사가 아닌 제목인지 확인
            if len(line) < 50 and not re.search(r'[.!?]', line):
                continue
        
        # 괄호 안의 설명 제거 (단, 가사의 일부인 경우 유지)
        cleaned_line = remove_explanatory_parentheses(line)
        
        # 빈 줄 처리
        if not cleaned_line.strip():
            cleaned_lines.append('')
        else:
            cleaned_lines.append(cleaned_line.strip())
    
    # 앞뒤 빈 줄 제거
    while cleaned_lines and cleaned_lines[0] == '':
        cleaned_lines.pop(0)
    while cleaned_lines and cleaned_lines[-1] == '':
        cleaned_lines.pop()
    
    return '\n'.join(cleaned_lines)

def remove_explanatory_parentheses(text):
    """
    설명적인 괄호만 제거하고 가사의 일부인 괄호는 유지
    """
    # 설명 키워드가 포함된 괄호 제거
    explanatory_keywords = [
        '설명', '추가', '참고', '주', '번역', '의역', '직역',
        'lit.', 'trans.', 'note', 'ref.'
    ]
    
    for keyword in explanatory_keywords:
        # (키워드 포함) 패턴 제거
        text = re.sub(rf'\([^)]*{re.escape(keyword)}[^)]*\)', '', text, flags=re.IGNORECASE)
        text = re.sub(rf'\[[^]]*{re.escape(keyword)}[^]]*\]', '', text, flags=re.IGNORECASE)
    
    # 여러 공백을 하나로
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

def is_pronunciation(text):
    """
    발음 텍스트인지 확인
    """
    if not text:
        return False
    
    # 한글 발음이 포함되어 있는지 확인
    has_korean = bool(re.search(r'[가-힣]', text))
    
    # 영어 원문이 너무 많이 포함되어 있는지 확인
    english_ratio = len(re.findall(r'[a-zA-Z]', text)) / max(len(text), 1)
    
    return has_korean and english_ratio < 0.5

def is_translation(text):
    """
    해석 텍스트인지 확인
    """
    if not text:
        return False
    
    # 한글이 포함되어 있는지 확인
    has_korean = bool(re.search(r'[가-힣]', text))
    
    return has_korean

def main():
    """메인 함수"""
    # cleaned_data 경로 사용
    csv_path = Path('/Users/youz2me/Xcode/Livith-Data/output/cleaned_data/songs.csv')
    
    print("=" * 70)
    print("📚 Songs Translation 필드 정리")
    print("=" * 70)
    print("\n📌 규칙:")
    print("  1. translation = '발음\\n\\n해석' 형식만 허용")
    print("  2. 원문(lyrics)은 제거 (별도 필드에 있음)")
    print("  3. 괄호 안 부가 설명 제거")
    print("  4. 곡 정보나 메타데이터 제거")
    print("  5. 순수 발음과 순수 해석만 유지")
    print()
    
    print(f"📁 파일 읽기: {csv_path}")
    df = pd.read_csv(csv_path, encoding='utf-8')
    
    print(f"  • 총 레코드: {len(df)}개")
    
    # 백업 생성
    backup_path = csv_path.with_suffix('.csv.backup')
    df.to_csv(backup_path, index=False, encoding='utf-8')
    print(f"  • 백업 생성: {backup_path}")
    
    # translation 필드가 있는 레코드 확인
    has_translation = df['translation'].notna() & (df['translation'] != '')
    translation_count = has_translation.sum()
    
    print(f"  • translation 있는 레코드: {translation_count}개")
    
    if translation_count == 0:
        print("\n⚠️ translation 필드가 비어있습니다.")
        return
    
    # translation 정리
    print("\n🔧 Translation 필드 정리 중...")
    
    success_count = 0
    empty_count = 0
    error_count = 0
    sample_outputs = []
    
    for idx, row in df[has_translation].iterrows():
        original = row['translation']
        
        # 정리
        cleaned = extract_pronunciation_and_translation(original)
        
        if cleaned:
            df.at[idx, 'translation'] = cleaned
            success_count += 1
            
            # 처음 3개 샘플 저장
            if len(sample_outputs) < 3:
                sample_outputs.append({
                    'title': row['title'],
                    'artist': row['artist'],
                    'original': original[:150] + '...' if len(str(original)) > 150 else original,
                    'cleaned': cleaned[:150] + '...' if len(cleaned) > 150 else cleaned
                })
        else:
            df.at[idx, 'translation'] = ''
            empty_count += 1
            
            if error_count < 3:  # 처음 3개 오류만 출력
                print(f"  ⚠️ [{idx}] {row['title']} - 유효한 형식 찾지 못함")
                error_count += 1
    
    print(f"\n📊 처리 결과:")
    print(f"  • 정리 성공: {success_count}개")
    print(f"  • 빈 값 처리: {empty_count}개")
    print(f"  • 총 처리: {success_count + empty_count}개")
    
    # 샘플 출력
    if sample_outputs:
        print("\n📋 정리된 샘플:")
        for i, sample in enumerate(sample_outputs, 1):
            print(f"\n[{i}] {sample['title']} by {sample['artist']}")
            print("-" * 50)
            print(f"원본:\n{sample['original']}\n")
            print(f"정리:\n{sample['cleaned']}")
    
    # 저장
    print(f"\n💾 저장 중: {csv_path}")
    df.to_csv(csv_path, index=False, encoding='utf-8')
    
    print("\n" + "=" * 70)
    print("✅ Translation 필드 정리 완료!")
    print(f"  • 파일: {csv_path}")
    print(f"  • 백업: {backup_path}")
    print(f"  • 정리된 레코드: {success_count}/{translation_count}개")
    print("=" * 70)

if __name__ == "__main__":
    main()