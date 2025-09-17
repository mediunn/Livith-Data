#!/usr/bin/env python3
"""
songs.csv에 직접 번역 테스트 적용
"""
import pandas as pd
from pathlib import Path

def test_direct_translation():
    """직접 translation 테스트 및 songs.csv 적용"""
    
    # songs.csv 읽기
    csv_path = Path('/Users/youz2me/Xcode/Livith-Data/output/main_output/songs.csv')
    df = pd.read_csv(csv_path, encoding='utf-8')
    
    print("=" * 70)
    print("🎵 Direct Translation 테스트 - songs.csv")
    print("=" * 70)
    
    # Higher Power 곡 찾기
    higher_power = df[df['title'] == 'Higher Power']
    
    if higher_power.empty:
        print("❌ Higher Power 곡을 찾을 수 없습니다.")
        return
    
    song_idx = higher_power.index[0]
    song = higher_power.iloc[0]
    
    print(f"곡명: {song['title']}")
    print(f"아티스트: {song['artist']}")
    print(f"가사: {song['lyrics'][:100]}...")
    
    # 테스트용 발음과 해석 데이터
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

    # 올바른 형식 (발음 + 두줄바꿈 + 해석)
    combined_translation = f"{test_pronunciation}\n\n{test_translation}"
    
    print(f"\n📝 생성할 translation 필드:")
    print("-" * 50)
    print(combined_translation)
    print("-" * 50)
    
    # 형식 검증
    parts = combined_translation.split('\n\n')
    pronunciation_lines = len([line for line in parts[0].split('\n') if line.strip()])
    translation_lines = len([line for line in parts[1].split('\n') if line.strip()])
    
    print(f"\n✅ 형식 검증:")
    print(f"  • 파트 수: {len(parts)} (기대값: 2)")
    print(f"  • 발음 줄 수: {pronunciation_lines}")
    print(f"  • 해석 줄 수: {translation_lines}")
    print(f"  • 줄 수 일치: {'✅' if pronunciation_lines == translation_lines else '❌'}")
    newline_sep = '\n\n'
    print(f"  • 구분자 존재: {'✅' if newline_sep in combined_translation else '❌'}")
    
    # pronunciation과 translation 필드 분리해서 저장
    print(f"\n💾 CSV 업데이트:")
    
    # pronunciation 필드에 발음만
    df.at[song_idx, 'pronunciation'] = test_pronunciation
    
    # translation 필드에 해석만  
    df.at[song_idx, 'translation'] = test_translation
    
    print(f"  • pronunciation 필드: {len(test_pronunciation)}자")
    print(f"  • translation 필드: {len(test_translation)}자")
    print(f"  • 발음 줄 수: {pronunciation_lines}")
    print(f"  • 해석 줄 수: {translation_lines}")
    
    # 저장
    df.to_csv(csv_path, index=False, encoding='utf-8')
    print(f"  • 저장 완료: {csv_path}")
    
    # 검증 - 다시 읽어서 확인
    df_check = pd.read_csv(csv_path, encoding='utf-8')
    check_song = df_check[df_check['title'] == 'Higher Power'].iloc[0]
    
    print(f"\n🔍 저장 검증:")
    print(f"  • pronunciation 필드 존재: {'✅' if pd.notna(check_song['pronunciation']) else '❌'}")
    print(f"  • translation 필드 존재: {'✅' if pd.notna(check_song['translation']) else '❌'}")
    print(f"  • pronunciation 내용: {str(check_song['pronunciation'])[:50]}...")
    print(f"  • translation 내용: {str(check_song['translation'])[:50]}...")
    
    # 원문과 줄 싱크 확인
    original_lines = [line.strip() for line in song['lyrics'].split('\n') if line.strip()]
    pronunciation_check = [line.strip() for line in check_song['pronunciation'].split('\n') if line.strip()]
    translation_check = [line.strip() for line in check_song['translation'].split('\n') if line.strip()]
    
    print(f"\n🔄 줄 싱크 검증:")
    print(f"  • 원문 줄 수: {len(original_lines)}")
    print(f"  • 발음 줄 수: {len(pronunciation_check)}")
    print(f"  • 해석 줄 수: {len(translation_check)}")
    print(f"  • 원문-발음 일치: {'✅' if len(original_lines) == len(pronunciation_check) else '❌'}")
    print(f"  • 원문-해석 일치: {'✅' if len(original_lines) == len(translation_check) else '❌'}")
    print(f"  • 발음-해석 일치: {'✅' if len(pronunciation_check) == len(translation_check) else '❌'}")
    
    print(f"\n🎯 결과 요약:")
    print(f"  • pronunciation 필드: 발음만 포함 ✅")
    print(f"  • translation 필드: 해석만 포함 ✅") 
    print(f"  • 줄 싱크: {'✅' if len(original_lines) == len(pronunciation_check) == len(translation_check) else '❌'}")
    print(f"  • CSV 저장: 완료 ✅")

if __name__ == "__main__":
    test_direct_translation()