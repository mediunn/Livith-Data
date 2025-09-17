#!/usr/bin/env python3
"""
원어(lyrics)가 있는 곡들에 발음과 해석을 추가하는 스크립트
Higher Power 등 가사가 있지만 pronunciation, translation이 비어있는 곡들 처리
"""
import pandas as pd
import os
import google.generativeai as genai
from pathlib import Path
import time
from dotenv import load_dotenv

def setup_gemini():
    """Gemini AI 설정"""
    # .env 파일에서 환경변수 로드
    load_dotenv()
    
    api_key = os.getenv('GEMINI_API_KEY')
    if not api_key:
        print("❌ GEMINI_API_KEY가 .env 파일에 설정되지 않았습니다.")
        print("   .env 파일에 다음과 같이 추가하세요:")
        print("   GEMINI_API_KEY=your_api_key_here")
        return None
    
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-1.5-flash')
    return model

def generate_pronunciation_and_translation(model, lyrics, title, artist):
    """AI를 사용하여 발음과 해석 생성"""
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
        print(f"❌ AI 생성 오류 ({title}): {e}")
        return None

def parse_ai_response(response_text):
    """AI 응답을 파싱하여 발음과 해석 추출"""
    if not response_text:
        return None, None
    
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
    
    pronunciation = '\n'.join(pronunciation_lines) if pronunciation_lines else None
    translation = '\n'.join(translation_lines) if translation_lines else None
    
    return pronunciation, translation

def main():
    """메인 실행 함수"""
    csv_path = Path('/Users/youz2me/Xcode/Livith-Data/output/main_output/songs.csv')
    
    if not csv_path.exists():
        print(f"❌ 파일을 찾을 수 없습니다: {csv_path}")
        return
    
    # CSV 읽기
    print("📁 CSV 파일 로드 중...")
    df = pd.read_csv(csv_path, encoding='utf-8')
    print(f"   총 {len(df)}개 곡 로드됨")
    
    # lyrics는 있지만 pronunciation이나 translation이 비어있는 곡들 찾기
    has_lyrics = df['lyrics'].notna() & (df['lyrics'] != '')
    needs_pronunciation = df['pronunciation'].isna() | (df['pronunciation'] == '')
    needs_translation = df['translation'].isna() | (df['translation'] == '')
    
    target_songs = df[has_lyrics & (needs_pronunciation | needs_translation)]
    
    print(f"🎯 번역이 필요한 곡: {len(target_songs)}개")
    
    if len(target_songs) == 0:
        print("✅ 모든 곡이 이미 번역되어 있습니다.")
        return
    
    # AI 모델 설정
    model = setup_gemini()
    if not model:
        return
    
    print("\n🤖 AI 번역 시작...")
    print("=" * 70)
    
    success_count = 0
    error_count = 0
    
    for idx, song in target_songs.iterrows():
        print(f"\n[{success_count + error_count + 1}/{len(target_songs)}] {song['title']} - {song['artist']}")
        
        # AI로 발음과 해석 생성
        ai_response = generate_pronunciation_and_translation(
            model, 
            song['lyrics'],
            song['title'],
            song['artist']
        )
        
        if ai_response:
            pronunciation, translation = parse_ai_response(ai_response)
            
            if pronunciation and translation:
                # 빈 필드만 업데이트
                if pd.isna(song['pronunciation']) or song['pronunciation'] == '':
                    df.at[idx, 'pronunciation'] = pronunciation
                    print(f"   ✅ 발음 추가됨")
                
                if pd.isna(song['translation']) or song['translation'] == '':
                    df.at[idx, 'translation'] = translation
                    print(f"   ✅ 해석 추가됨")
                
                success_count += 1
            else:
                print(f"   ❌ 파싱 실패")
                error_count += 1
        else:
            error_count += 1
        
        # API 제한 방지를 위한 대기
        time.sleep(1)
    
    print("\n" + "=" * 70)
    print(f"📊 처리 완료: 성공 {success_count}개, 실패 {error_count}개")
    
    if success_count > 0:
        # 백업 생성
        backup_path = csv_path.parent / f"songs_backup_{int(time.time())}.csv"
        df.to_csv(backup_path, index=False, encoding='utf-8')
        print(f"💾 백업 생성: {backup_path}")
        
        # 원본 파일 업데이트
        df.to_csv(csv_path, index=False, encoding='utf-8')
        print(f"✅ 파일 업데이트 완료: {csv_path}")
    else:
        print("❌ 업데이트된 데이터가 없습니다.")

if __name__ == "__main__":
    main()