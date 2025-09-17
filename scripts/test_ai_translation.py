#!/usr/bin/env python3
"""
AI를 사용하여 곡 가사의 발음과 해석을 생성하는 테스트 스크립트
"""
import pandas as pd
import os
import google.generativeai as genai
from pathlib import Path

def setup_gemini():
    """Gemini AI 설정"""
    api_key = os.getenv('GEMINI_API_KEY')
    if not api_key:
        print("❌ GEMINI_API_KEY 환경변수가 설정되지 않았습니다.")
        return None
    
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-1.5-flash')
    return model

def generate_pronunciation_and_translation(model, lyrics, title, artist):
    """
    AI를 사용하여 발음과 해석 생성
    """
    prompt = f"""다음 영어 가사에 대해 한국어 발음과 한국어 해석을 생성해주세요.

곡명: {title}
아티스트: {artist}

가사:
{lyrics}

요구사항:
1. 발음: 영어를 한국어로 음성학적 표기 (예: "I feel good" → "아이 필 굿")
2. 해석: 자연스러운 한국어 번역
3. 줄바꿈을 원문과 동일하게 유지
4. 괄호 안의 부가 설명은 절대 포함하지 마세요
5. 곡에 대한 설명이나 부가 정보는 포함하지 마세요

응답 형식 (정확히 이 형태로):
발음:
[발음 내용]

해석:
[해석 내용]"""

    try:
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        print(f"❌ AI 생성 오류: {e}")
        return None

def parse_ai_response(response_text):
    """
    AI 응답을 파싱하여 발음과 해석 추출
    """
    if not response_text:
        return None
    
    lines = response_text.strip().split('\n')
    
    pronunciation_lines = []
    translation_lines = []
    current_section = None
    
    for line in lines:
        line = line.strip()
        
        if line.startswith('발음:') or line.lower().startswith('pronunciation:'):
            current_section = 'pronunciation'
            continue
        elif line.startswith('해석:') or line.lower().startswith('translation:'):
            current_section = 'translation'
            continue
        
        if current_section == 'pronunciation' and line:
            pronunciation_lines.append(line)
        elif current_section == 'translation' and line:
            translation_lines.append(line)
    
    if pronunciation_lines and translation_lines:
        pronunciation = '\n'.join(pronunciation_lines)
        translation = '\n'.join(translation_lines)
        return f"{pronunciation}\n\n{translation}"
    
    return None

def test_single_song():
    """단일 곡 테스트"""
    # 파일 읽기
    csv_path = Path('/Users/youz2me/Xcode/Livith-Data/output/cleaned_data/songs.csv')
    df = pd.read_csv(csv_path, encoding='utf-8')
    
    # lyrics가 있는 곡 중 하나 선택 (짧은 가사 우선)
    has_lyrics = df['lyrics'].notna() & (df['lyrics'] != '')
    lyrics_df = df[has_lyrics].copy()
    
    # 가사 길이 계산
    lyrics_df['lyrics_length'] = lyrics_df['lyrics'].str.len()
    
    # 중간 길이의 곡 선택 (너무 짧지도 길지도 않은)
    medium_length = lyrics_df[(lyrics_df['lyrics_length'] > 200) & (lyrics_df['lyrics_length'] < 800)]
    
    if medium_length.empty:
        test_song = lyrics_df.iloc[0]
    else:
        test_song = medium_length.iloc[0]
    
    print("=" * 70)
    print("🎵 AI Translation 테스트")
    print("=" * 70)
    print(f"곡명: {test_song['title']}")
    print(f"아티스트: {test_song['artist']}")
    print(f"가사 길이: {len(str(test_song['lyrics']))}자")
    print()
    
    # AI 모델 설정
    model = setup_gemini()
    if not model:
        return
    
    print("🤖 AI로 발음과 해석 생성 중...")
    
    # AI 생성
    ai_response = generate_pronunciation_and_translation(
        model, 
        test_song['lyrics'],
        test_song['title'],
        test_song['artist']
    )
    
    if not ai_response:
        print("❌ AI 응답 생성 실패")
        return
    
    print("\n📝 AI 원본 응답:")
    print(ai_response)
    
    # 응답 파싱
    parsed_result = parse_ai_response(ai_response)
    
    if parsed_result:
        print("\n✅ 파싱된 결과 (translation 필드 형식):")
        print("-" * 50)
        print(parsed_result)
        
        # CSV 업데이트 시뮬레이션
        print("\n💾 CSV 업데이트 시뮬레이션:")
        print(f"  • 곡 인덱스: {test_song.name}")
        print(f"  • translation 필드 길이: {len(parsed_result)}자")
        print("  • 형식 검증: '발음\\n\\n해석' ✅" if "\\n\\n" in parsed_result else "  • 형식 검증: 오류 ❌")
        
    else:
        print("❌ 응답 파싱 실패")

if __name__ == "__main__":
    test_single_song()