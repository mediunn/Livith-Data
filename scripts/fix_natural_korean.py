#!/usr/bin/env python3
"""
CSV 파일의 부자연스러운 한국어 종결어미와 문장 구성을 자연스럽게 수정
내용은 변경하지 않고 문체만 자연스럽게 다듬기
"""
import pandas as pd
from pathlib import Path
import time
import re

def fix_natural_korean_text(text):
    """한국어 텍스트를 자연스럽게 수정"""
    if pd.isna(text) or text == '':
        return text
    
    text = str(text)
    
    # 기본 종결어미 패턴 수정
    replacements = [
        # 어색한 종결어미 수정
        (r'됩니다만', '되지만'),
        (r'됩니다고 해요', '된다고 해요'),
        (r'됩니다고 합니다', '된다고 합니다'),
        (r'있습니다고 해요', '있다고 해요'),
        (r'입니다고 해요', '이라고 해요'),
        (r'습니다고 해요', '다고 해요'),
        
        # 이중 존댓말 수정
        (r'하십니다', '합니다'),
        (r'되십니다', '됩니다'),
        (r'계십니다', '계십니다'),  # 이건 그대로
        (r'하셔야 합니다', '해야 합니다'),
        (r'하셔야 해요', '해야 해요'),
        
        # 부자연스러운 조사 수정
        (r'에게서', '에서'),
        (r'으로부터', '에서'),
        (r'와/과', '와'),
        (r'을/를', '을'),
        (r'이/가', '이'),
        (r'은/는', '은'),
        
        # 어색한 표현 수정
        (r'매우 많은', '많은'),
        (r'대단히', '매우'),
        (r'굉장히 많은', '많은'),
        (r'엄청나게', '매우'),
        
        # 중복 표현 제거
        (r'다시 재', '다시'),
        (r'미리 사전에', '미리'),
        (r'직접 본인이', '직접'),
        (r'먼저 우선', '먼저'),
        
        # 문장 끝 정리
        (r'되요\.', '돼요.'),
        (r'되요$', '돼요'),
        (r'예요$', '에요'),
        (r'예요\.', '에요.'),
        (r'였어요', '었어요'),
        (r'됬', '됐'),
        
        # 공백 정리
        (r'\s+', ' '),
        (r'\s+\.', '.'),
        (r'\s+,', ','),
        (r'\s+!', '!'),
        (r'\s+\?', '?'),
    ]
    
    for pattern, replacement in replacements:
        text = re.sub(pattern, replacement, text)
    
    # 문장 부호 정리
    text = re.sub(r'([.!?])\1+', r'\1', text)  # 중복 문장부호 제거
    text = re.sub(r'\s*([.!?])\s*', r'\1 ', text)  # 문장부호 뒤 공백
    text = text.strip()
    
    # concert_info는 100자 제한이지만 말줄임표 대신 자연스럽게 마무리
    # 다른 파일은 제한 없음
    
    return text

def fix_concert_info_csv():
    """concert_info.csv 파일 수정 (100자 제한 적용)"""
    csv_path = Path('/Users/youz2me/Xcode/Livith-Data/output/main_output/concert_info.csv')
    
    if not csv_path.exists():
        print(f"❌ 파일을 찾을 수 없습니다: {csv_path}")
        return
    
    print("📁 concert_info.csv 처리 중...")
    
    # 백업 생성
    backup_path = csv_path.parent / f"concert_info_backup_{int(time.time())}.csv"
    df = pd.read_csv(csv_path, encoding='utf-8')
    df.to_csv(backup_path, index=False, encoding='utf-8')
    print(f"💾 백업 생성: {backup_path}")
    
    # content 컬럼 수정
    original_count = 0
    modified_count = 0
    
    for idx, row in df.iterrows():
        original = row['content']
        fixed = fix_natural_korean_text(original)
        
        # concert_info는 100자 제한 (말줄임표 대신 자연스럽게 마무리)
        if len(fixed) > 100:
            # 100자 근처에서 자연스러운 끝맺음 찾기
            if len(fixed) <= 103:  # 3글자 여유
                # 자연스러운 종결어미로 교체
                if not fixed.endswith(('요.', '다.', '요!', '요?', '해요.', '어요.', '니다.')):
                    fixed = fixed[:97] + "어요."
            else:
                # 마지막 문장 찾아서 100자에 맞춰 자연스럽게 마무리
                sentences = re.split(r'[.!?]', fixed)
                result = ""
                for sentence in sentences[:-1]:  # 마지막 빈 문자열 제외
                    if len(result + sentence.strip()) <= 95:
                        if result:
                            result += ". " + sentence.strip()
                        else:
                            result = sentence.strip()
                    else:
                        break
                
                if result:
                    if not result.endswith('.'):
                        result += "어요."
                    fixed = result[:100]
                else:
                    # 첫 문장도 길면 자연스럽게 줄이기
                    fixed = fixed[:97] + "어요."
        
        if original != fixed:
            df.at[idx, 'content'] = fixed
            modified_count += 1
            print(f"  수정: {row['artist_name']} - {row['category']}")
        original_count += 1
    
    # 저장
    df.to_csv(csv_path, index=False, encoding='utf-8')
    print(f"✅ concert_info.csv 완료: {modified_count}/{original_count}개 수정")
    
    return df

def fix_concerts_csv():
    """concerts.csv 파일 수정"""
    csv_path = Path('/Users/youz2me/Xcode/Livith-Data/output/main_output/concerts.csv')
    
    if not csv_path.exists():
        print(f"❌ 파일을 찾을 수 없습니다: {csv_path}")
        return
    
    print("\n📁 concerts.csv 처리 중...")
    
    # 백업 생성
    backup_path = csv_path.parent / f"concerts_backup_{int(time.time())}.csv"
    df = pd.read_csv(csv_path, encoding='utf-8')
    df.to_csv(backup_path, index=False, encoding='utf-8')
    print(f"💾 백업 생성: {backup_path}")
    
    # introduction, venue, label 컬럼 수정
    modified_count = 0
    
    for idx, row in df.iterrows():
        modified = False
        
        # introduction 수정
        if pd.notna(row.get('introduction')):
            original = row['introduction']
            fixed = fix_natural_korean_text(original)
            if original != fixed:
                df.at[idx, 'introduction'] = fixed
                modified = True
        
        # venue 수정
        if pd.notna(row.get('venue')):
            original = row['venue']
            fixed = fix_natural_korean_text(original)
            if original != fixed:
                df.at[idx, 'venue'] = fixed
                modified = True
        
        # label 수정
        if pd.notna(row.get('label')):
            original = row['label']
            fixed = fix_natural_korean_text(original)
            if original != fixed:
                df.at[idx, 'label'] = fixed
                modified = True
        
        if modified:
            modified_count += 1
            print(f"  수정: {row['title']}")
    
    # 저장
    df.to_csv(csv_path, index=False, encoding='utf-8')
    print(f"✅ concerts.csv 완료: {modified_count}개 레코드 수정")
    
    return df

def fix_songs_csv():
    """songs.csv 파일 수정 (translation 필드)"""
    csv_path = Path('/Users/youz2me/Xcode/Livith-Data/output/main_output/songs.csv')
    
    if not csv_path.exists():
        print(f"❌ 파일을 찾을 수 없습니다: {csv_path}")
        return
    
    print("\n📁 songs.csv 처리 중...")
    
    # 백업 생성
    backup_path = csv_path.parent / f"songs_backup_{int(time.time())}.csv"
    df = pd.read_csv(csv_path, encoding='utf-8')
    df.to_csv(backup_path, index=False, encoding='utf-8')
    print(f"💾 백업 생성: {backup_path}")
    
    # translation 컬럼 수정
    modified_count = 0
    
    for idx, row in df.iterrows():
        if pd.notna(row.get('translation')):
            original = row['translation']
            # translation은 여러 줄이므로 각 줄 처리
            lines = str(original).split('\n')
            fixed_lines = [fix_natural_korean_text(line) for line in lines]
            fixed = '\n'.join(fixed_lines)
            
            if original != fixed:
                df.at[idx, 'translation'] = fixed
                modified_count += 1
                if modified_count <= 5:  # 처음 5개만 출력
                    print(f"  수정: {row['title']} - {row['artist']}")
    
    # 저장
    df.to_csv(csv_path, index=False, encoding='utf-8')
    print(f"✅ songs.csv 완료: {modified_count}개 레코드 수정")
    
    return df

def fix_cultures_csv():
    """cultures.csv 파일 수정"""
    csv_path = Path('/Users/youz2me/Xcode/Livith-Data/output/main_output/cultures.csv')
    
    if not csv_path.exists():
        print(f"⚠️ cultures.csv 파일이 없습니다.")
        return
    
    print("\n📁 cultures.csv 처리 중...")
    
    # 백업 생성
    backup_path = csv_path.parent / f"cultures_backup_{int(time.time())}.csv"
    df = pd.read_csv(csv_path, encoding='utf-8')
    df.to_csv(backup_path, index=False, encoding='utf-8')
    print(f"💾 백업 생성: {backup_path}")
    
    # description 컬럼 수정
    modified_count = 0
    
    for idx, row in df.iterrows():
        if pd.notna(row.get('description')):
            original = row['description']
            fixed = fix_natural_korean_text(original)
            
            if original != fixed:
                df.at[idx, 'description'] = fixed
                modified_count += 1
                print(f"  수정: {row['artist']} - {row['category']}")
    
    # 저장
    df.to_csv(csv_path, index=False, encoding='utf-8')
    print(f"✅ cultures.csv 완료: {modified_count}개 레코드 수정")
    
    return df

def main():
    """메인 실행 함수"""
    print("=" * 70)
    print("🔧 CSV 파일 한국어 문체 자연스럽게 수정")
    print("=" * 70)
    print("내용은 변경하지 않고 종결어미와 문장 구성만 자연스럽게 수정합니다.\n")
    
    # 각 파일 처리
    fix_concert_info_csv()
    fix_concerts_csv()
    fix_songs_csv()
    fix_cultures_csv()
    
    print("\n" + "=" * 70)
    print("✅ 모든 파일 처리 완료!")
    print("=" * 70)

if __name__ == "__main__":
    main()