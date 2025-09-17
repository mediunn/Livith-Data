#!/usr/bin/env python3
"""
pronunciation 필드를 translation 필드로 변환하는 스크립트
pronunciation에 발음과 해석이 섞여 있는 경우를 처리
"""
import pandas as pd
import re
from pathlib import Path

def convert_pronunciation_to_translation(pronunciation_text):
    """
    pronunciation 필드에서 발음과 해석을 분리하여 translation 형식으로 변환
    
    Args:
        pronunciation_text: pronunciation 필드 내용
        
    Returns:
        "발음\n\n해석" 형식의 문자열
    """
    if pd.isna(pronunciation_text) or not pronunciation_text:
        return ''
    
    text = str(pronunciation_text).strip()
    lines = text.split('\n')
    
    pronunciation_lines = []
    translation_lines = []
    current_section = 'pronunciation'  # 기본적으로 발음 섹션
    
    for line in lines:
        line = line.strip()
        
        # 빈 줄 처리
        if not line:
            if current_section == 'pronunciation':
                pronunciation_lines.append('')
            else:
                translation_lines.append('')
            continue
        
        # 발음인지 해석인지 판단
        # 한글 음성학적 표기 (예: 리빈, 에브리띵, 헤븐리)
        is_pronunciation = is_phonetic_korean(line)
        
        # 자연스러운 한국어 문장 (예: 모든 것이 너무 아름답고 달콤해 보여)
        is_translation = is_natural_korean(line)
        
        # 판단 결과에 따라 분류
        if is_pronunciation and not is_translation:
            pronunciation_lines.append(line)
        elif is_translation and not is_pronunciation:
            translation_lines.append(line)
        else:
            # 애매한 경우는 현재 섹션에 추가
            if current_section == 'pronunciation':
                pronunciation_lines.append(line)
            else:
                translation_lines.append(line)
    
    # 발음과 해석이 모두 있어야 유효
    pronunciation = '\n'.join(pronunciation_lines).strip()
    translation = '\n'.join(translation_lines).strip()
    
    if pronunciation and translation:
        return f"{pronunciation}\n\n{translation}"
    
    # 발음만 있는 경우 빈 문자열 반환 (해석이 없으면 무의미)
    return ''

def is_phonetic_korean(text):
    """
    한글 음성학적 표기인지 판단
    (예: 리빈, 에브리띵, 헤븐리 등)
    """
    if not text or not re.search(r'[가-힣]', text):
        return False
    
    # 음성학적 표기의 특징
    phonetic_patterns = [
        r'[가-힣]+[아이우에오][가-힣]*',  # 발음 기호적 패턴
        r'띵|딘|린|빈|니|티|시',  # 영어 발음의 한글 표기 특징
        r'더\s+[가-힣]+',  # "더 + 단어" 패턴 (영어 정관사 the)
        r'인\s+[가-힣]+',  # "인 + 단어" 패턴 (영어 전치사 in)
        r'앤\s+[가-힣]+',  # "앤 + 단어" 패턴 (영어 접속사 and)
    ]
    
    # 패턴 매칭 확인
    for pattern in phonetic_patterns:
        if re.search(pattern, text):
            return True
    
    # 단어 길이가 짧고 자연스럽지 않은 한글 조합
    words = text.split()
    short_unnatural_count = 0
    for word in words:
        if len(word) <= 4 and re.match(r'^[가-힣]+$', word):
            # 자연스러운 한국어 단어가 아닌 경우
            if not is_natural_korean_word(word):
                short_unnatural_count += 1
    
    # 부자연스러운 단어가 많으면 발음으로 판단
    return short_unnatural_count > len(words) * 0.3

def is_natural_korean(text):
    """
    자연스러운 한국어 문장인지 판단
    """
    if not text or not re.search(r'[가-힣]', text):
        return False
    
    # 자연스러운 한국어 문장의 특징
    natural_patterns = [
        r'[가-힣]+[이가는을를]',  # 조사 사용
        r'[가-힣]+[다요해]$',  # 문장 종결어미
        r'너무|정말|매우|아주|참|그냥',  # 부사
        r'이다|있다|없다|되다|하다',  # 서술어
        r'그래서|그런데|하지만|그리고',  # 접속어
    ]
    
    for pattern in natural_patterns:
        if re.search(pattern, text):
            return True
    
    return False

def is_natural_korean_word(word):
    """
    자연스러운 한국어 단어인지 판단
    """
    # 일반적인 한국어 단어들
    common_words = {
        '모든', '것이', '너무', '아름', '답고', '달콤', '해요', '보여', '처럼',
        '생활', '컬러', '주변', '모습', '밝게', '영화', '장면', '색깔', '그림'
    }
    
    return word in common_words or len(word) > 4

def main():
    """메인 함수"""
    csv_path = Path('/Users/youz2me/Xcode/Livith-Data/output/cleaned_data/songs.csv')
    
    print("=" * 70)
    print("🔄 Pronunciation → Translation 변환")
    print("=" * 70)
    
    print(f"📁 파일 읽기: {csv_path}")
    df = pd.read_csv(csv_path, encoding='utf-8')
    
    # pronunciation 필드가 있는 레코드 확인
    has_pronunciation = df['pronunciation'].notna() & (df['pronunciation'] != '')
    pronunciation_count = has_pronunciation.sum()
    
    print(f"  • 총 레코드: {len(df)}개")
    print(f"  • pronunciation 있는 레코드: {pronunciation_count}개")
    
    if pronunciation_count == 0:
        print("\n⚠️ pronunciation 필드가 비어있습니다.")
        return
    
    # 변환 처리
    print(f"\n🔄 pronunciation → translation 변환 중...")
    
    success_count = 0
    
    for idx, row in df[has_pronunciation].iterrows():
        pronunciation_text = row['pronunciation']
        
        # 변환
        translation_text = convert_pronunciation_to_translation(pronunciation_text)
        
        if translation_text:
            df.at[idx, 'translation'] = translation_text
            df.at[idx, 'pronunciation'] = ''  # pronunciation 필드 비우기
            success_count += 1
            
            print(f"  ✅ [{idx}] {row['title']} - 변환 완료")
        else:
            print(f"  ⚠️ [{idx}] {row['title']} - 발음과 해석 분리 실패")
    
    print(f"\n📊 변환 결과:")
    print(f"  • 성공: {success_count}개")
    print(f"  • 실패: {pronunciation_count - success_count}개")
    
    # 저장
    print(f"\n💾 저장 중: {csv_path}")
    df.to_csv(csv_path, index=False, encoding='utf-8')
    
    # 결과 확인
    final_translation_count = (df['translation'].notna() & (df['translation'] != '')).sum()
    
    print(f"\n✅ 변환 완료!")
    print(f"  • 최종 translation 레코드: {final_translation_count}개")
    
    # 샘플 출력
    if final_translation_count > 0:
        print(f"\n📋 변환된 샘플:")
        sample = df[df['translation'].notna() & (df['translation'] != '')].iloc[0]
        print(f"곡: {sample['title']} by {sample['artist']}")
        print("-" * 50)
        translation_preview = sample['translation'][:300] + '...' if len(sample['translation']) > 300 else sample['translation']
        print(translation_preview)

if __name__ == "__main__":
    main()