#!/usr/bin/env python3
"""
concert_info.csv에서 잘린 텍스트를 자연스럽게 수정하는 스크립트
최대 글자수 100자 제한으로 잘린 내용들을 AI로 자연스럽게 완성
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

def complete_truncated_text(model, artist_name, concert_title, category, truncated_content):
    """AI를 사용하여 잘린 텍스트를 자연스럽게 완성 (100자 제한)"""
    
    prompt = f"""다음은 콘서트 정보 중 잘린 텍스트입니다. 자연스럽게 완성해주세요.

아티스트: {artist_name}
콘서트명: {concert_title}
카테고리: {category}

잘린 내용:
{truncated_content}

요구사항:
1. 현재 내용의 맥락을 유지하면서 자연스럽게 완성
2. 콘서트 정보 안내 톤으로 작성 (정중하고 친근함)
3. **반드시 전체 100자 이내로 완성** (매우 중요!)
4. 원본의 어투와 스타일 유지 ("~예요", "~해요" 등)
5. 추측이나 허위 정보는 포함하지 말고, 일반적이고 합리적인 완성만
6. 기존 내용은 절대 변경하지 말고 뒤에 최소한만 추가해서 자연스럽게 마무리
7. 100자를 넘기지 않기 위해 필요하면 내용을 간결하게 요약

완성된 전체 텍스트만 응답해주세요. 반드시 100자 이내여야 합니다."""

    try:
        response = model.generate_content(prompt)
        completed_text = response.text.strip()
        
        # 100자 초과 시 잘라내기
        if len(completed_text) > 100:
            # 100자에서 자연스러운 끝맺음 찾기
            completed_text = completed_text[:97] + "요."
        
        # 100자 정확히 맞추기
        if len(completed_text) < 100 and len(completed_text) > len(truncated_content):
            return completed_text
        elif len(completed_text) > 100:
            return completed_text[:100]
        else:
            # 원본이 이미 충분히 길거나 AI 응답이 짧으면 간단히 마무리
            remaining = 100 - len(truncated_content.rstrip())
            if remaining > 2:
                return truncated_content.rstrip() + "요."
            else:
                return truncated_content[:100]
            
    except Exception as e:
        print(f"❌ AI 생성 오류 ({artist_name}): {e}")
        # 오류 시 간단하게 마무리만 추가 (100자 제한)
        if len(truncated_content) < 98:
            return truncated_content.rstrip() + "요."
        else:
            return truncated_content[:100]

def is_truncated(content):
    """텍스트가 잘린 것인지 판단"""
    content = str(content).strip()
    
    # 100자 정확히이고 자연스럽지 않은 끝맺음
    if len(content) == 100:
        natural_endings = ['.', '요', '다', '해요', '니다', '어요', '습니다', '네요', '됩니다', '!', '?', '세요']
        return not any(content.endswith(ending) for ending in natural_endings)
    
    return False

def main():
    """메인 실행 함수"""
    csv_path = Path('/Users/youz2me/Xcode/Livith-Data/output/main_output/concert_info.csv')
    
    if not csv_path.exists():
        print(f"❌ 파일을 찾을 수 없습니다: {csv_path}")
        return
    
    # CSV 읽기
    print("📁 CSV 파일 로드 중...")
    df = pd.read_csv(csv_path, encoding='utf-8')
    print(f"   총 {len(df)}개 레코드 로드됨")
    
    # 잘린 텍스트 찾기
    truncated_rows = []
    for idx, row in df.iterrows():
        if is_truncated(row['content']):
            truncated_rows.append(idx)
    
    print(f"🎯 잘린 텍스트 발견: {len(truncated_rows)}개")
    
    if len(truncated_rows) == 0:
        print("✅ 잘린 텍스트가 없습니다.")
        return
    
    # AI 모델 설정
    model = setup_gemini()
    if not model:
        return
    
    print("\n🤖 AI로 텍스트 완성 중...")
    print("=" * 70)
    
    success_count = 0
    error_count = 0
    
    # 백업 생성
    backup_path = csv_path.parent / f"concert_info_backup_{int(time.time())}.csv"
    df.to_csv(backup_path, index=False, encoding='utf-8')
    print(f"💾 백업 생성: {backup_path}")
    print()
    
    for count, idx in enumerate(truncated_rows, 1):
        row = df.iloc[idx]
        print(f"[{count}/{len(truncated_rows)}] {row['artist_name']} - {row['concert_title']}")
        print(f"   카테고리: {row['category']}")
        print(f"   기존 길이: {len(str(row['content']))}자")
        print(f"   기존 내용: ...{str(row['content'])[-30:]}")
        
        # AI로 텍스트 완성
        completed_content = complete_truncated_text(
            model,
            row['artist_name'],
            row['concert_title'], 
            row['category'],
            row['content']
        )
        
        if completed_content and completed_content != row['content']:
            # 100자 제한 확인
            if len(completed_content) > 100:
                print(f"   ⚠️  100자 초과 ({len(completed_content)}자), 100자로 조정")
                completed_content = completed_content[:100]
            
            df.at[idx, 'content'] = completed_content
            print(f"   ✅ 완성됨: {len(completed_content)}자")
            print(f"   새 내용: ...{completed_content[-40:]}")
            success_count += 1
        else:
            print(f"   ❌ 완성 실패 또는 변경 없음")
            error_count += 1
        
        print()
        
        # API 제한 방지를 위한 대기
        time.sleep(1)
    
    print("=" * 70)
    print(f"📊 처리 완료: 성공 {success_count}개, 실패 {error_count}개")
    
    if success_count > 0:
        # 원본 파일 업데이트
        df.to_csv(csv_path, index=False, encoding='utf-8')
        print(f"✅ 파일 업데이트 완료: {csv_path}")
        
        # 최종 통계
        print("\n📈 업데이트 후 통계:")
        content_lengths = df['content'].str.len()
        print(f"   평균 글자 수: {content_lengths.mean():.1f}자")
        print(f"   최대 글자 수: {content_lengths.max()}자")
        print(f"   100자 정확히: {(content_lengths == 100).sum()}개")
        print(f"   100자 초과: {(content_lengths > 100).sum()}개 (없어야 정상)")
        
        # 100자 초과 확인
        over_100 = df[df['content'].str.len() > 100]
        if len(over_100) > 0:
            print("\n⚠️  경고: 100자 초과 항목 발견!")
            for idx, row in over_100.iterrows():
                print(f"   - {row['artist_name']}: {len(row['content'])}자")
        
    else:
        print("❌ 업데이트된 데이터가 없습니다.")

if __name__ == "__main__":
    main()