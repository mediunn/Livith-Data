#!/usr/bin/env python3
"""
수동으로 translation 필드 형식을 테스트하는 스크립트
"""
import pandas as pd
from pathlib import Path

def test_translation_format():
    """translation 필드 형식 테스트"""
    
    # 테스트 데이터 (예시)
    test_pronunciation = """앰 아이 클로져 햅 아이 로스트 마이 마인드
더 선 이즈 버닝 미
이즈 잇 브로큰 데어즈 어 리프트 인 타임
쇼드 아이 테이크 디핏
이븐 이프 아이 파인드 어 하이어 파워
썸띵 텔스 미 잇 원트 비 이너프
앤드 이븐 이프 디스"""

    test_translation = """내가 더 가까워졌을까? 내가 정신을 잃었을까?
태양이 나를 태우고 있어
망가진 걸까? 시간에 균열이 있어
내가 패배를 받아들여야 할까?
더 높은 힘을 찾더라도
뭔가 그것으로는 충분하지 않을 거라고 말하고 있어
그리고 이것이"""

    # 올바른 형식으로 결합
    correct_format = f"{test_pronunciation}\n\n{test_translation}"
    
    print("=" * 70)
    print("🧪 Translation 필드 형식 테스트")
    print("=" * 70)
    
    print("📝 생성된 translation 필드:")
    print("-" * 50)
    print(correct_format)
    print("-" * 50)
    
    # 형식 검증
    parts = correct_format.split('\n\n')
    
    print(f"\n✅ 형식 검증:")
    print(f"  • 파트 수: {len(parts)} (기대값: 2)")
    print(f"  • 발음 파트 길이: {len(parts[0])}자")
    print(f"  • 해석 파트 길이: {len(parts[1])}자")
    newline_separator = '\\n\\n'
    has_separator = '포함됨' if '\n\n' in correct_format else '없음'
    print(f"  • 줄바꿈 구분자: '{newline_separator}' ({has_separator})")
    
    # 줄 수 확인
    pronunciation_lines = len([line for line in parts[0].split('\n') if line.strip()])
    translation_lines = len([line for line in parts[1].split('\n') if line.strip()])
    
    print(f"  • 발음 줄 수: {pronunciation_lines}")
    print(f"  • 해석 줄 수: {translation_lines}")
    print(f"  • 줄 수 일치: {'✅' if pronunciation_lines == translation_lines else '❌'}")
    
    # CSV 업데이트 시뮬레이션
    print(f"\n💾 CSV 업데이트 시뮬레이션:")
    
    csv_path = Path('/Users/youz2me/Xcode/Livith-Data/output/cleaned_data/songs.csv')
    df = pd.read_csv(csv_path, encoding='utf-8')
    
    # Higher Power 곡 찾기
    target_song = df[df['title'] == 'Higher Power'].iloc[0] if not df[df['title'] == 'Higher Power'].empty else df.iloc[0]
    
    print(f"  • 대상 곡: {target_song['title']} by {target_song['artist']}")
    print(f"  • 인덱스: {target_song.name}")
    
    # 실제로 업데이트해보기
    df_copy = df.copy()
    df_copy.at[target_song.name, 'translation'] = correct_format
    
    # 검증
    updated_value = df_copy.at[target_song.name, 'translation']
    
    print(f"  • 업데이트됨: {'✅' if updated_value == correct_format else '❌'}")
    print(f"  • 저장 준비: {'✅' if len(updated_value) > 0 else '❌'}")
    
    # 저장하지 않고 테스트만
    print(f"\n📊 결과:")
    print(f"  • 형식 올바름: {'✅' if len(parts) == 2 else '❌'}")
    print(f"  • 내용 유효함: {'✅' if parts[0] and parts[1] else '❌'}")
    has_newlines = '✅' if '\n' in parts[0] and '\n' in parts[1] else '❌'
    print(f"  • 줄바꿈 보존: {has_newlines}")
    
    return correct_format

def show_requirements():
    """요구사항 요약"""
    print(f"\n" + "=" * 70)
    print("📋 Translation 필드 요구사항 요약")
    print("=" * 70)
    
    requirements = [
        "1. 형식: '발음(두줄바꿈)해석' (정확히 두 줄바꿈으로 구분)",
        "2. 발음: 영어의 한글 음성학적 표기 (부가 설명 없음)",  
        "3. 해석: 자연스러운 한국어 번역 (부가 설명 없음)",
        "4. 줄바꿈: 원문과 발음, 해석의 줄 구성이 동일해야 함",
        "5. 순수성: 곡 정보, 괄호 설명, 메타데이터 포함 금지",
        "6. 일관성: 모든 곡에 동일한 형식 적용"
    ]
    
    for req in requirements:
        print(f"  {req}")
    
    print(f"\n✅ 스크립트 준비 완료:")
    print(f"  • 정리 스크립트: clean_songs_translation.py")
    print(f"  • 테스트 스크립트: manual_test_translation.py") 
    print(f"  • AI 생성 스크립트: test_ai_translation.py (API 키 필요)")

if __name__ == "__main__":
    test_format = test_translation_format()
    show_requirements()
    
    print(f"\n🎯 다음 단계:")
    print(f"  1. 유효한 AI API 키 확보")
    print(f"  2. 214개 곡에 대해 AI 생성 실행")
    print(f"  3. 생성된 내용 형식 검증")
    print(f"  4. MySQL에 업로드")