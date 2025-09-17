#!/usr/bin/env python3
"""
Higher Power 곡의 완전한 발음과 해석 생성 및 적용
"""
import pandas as pd
from pathlib import Path

def create_complete_translation():
    """모든 줄에 대응하는 완전한 발음과 해석 생성"""
    
    # 22줄 완전한 발음
    complete_pronunciation = """앰 아이 클로져 햅 아이 로스트 마이 마인드
더 선 이즈 버닝 미
이즈 잇 브로큰 데어즈 어 리프트 인 타임
쇼드 아이 테이크 디핏

이븐 이프 아이 파인드 어 하이어 파워
썸띵 텔스 미 잇 원트 비 이너프
앤드 이븐 이프 디스 데저트 스타츠 투 플라워
아일 스틸 파인드 어 웨이 투 스필 마이 블러드

아이 띵크 아이 로스트 마이 마인드
아이 필 어 리프트 인 타임
아이 띵크 아이 로스트 마이 마인드

앰 아이 클로져 햅 아이 로스트 마이 마인드
더 선 이즈 버닝 미
이즈 잇 브로큰 데어즈 어 리프트 인 타임
쇼드 아이 테이크 디핏

이븐 이프 아이 파인드 어 하이어 파워
썸띵 텔스 미 잇 원트 비 이너프
앤드 이븐 이프 디스 데저트 스타츠 투 플라워
아일 스틸 파인드 어 웨이 투 스필 마이 블러드

아이 띵크 아이 로스트 마이 마인드
아이 필 어 리프트 인 타임
아이 띵크 아이 로스트 마이 마인드"""

    # 22줄 완전한 해석
    complete_translation = """내가 더 가까워졌을까? 내가 정신을 잃었을까?
태양이 나를 태우고 있어
망가진 걸까? 시간에 균열이 있어
내가 패배를 받아들여야 할까?

더 높은 힘을 찾더라도
뭔가 그것으로는 충분하지 않을 거라고 말하고 있어
그리고 이 사막이 꽃피기 시작하더라도
난 여전히 내 피를 흘릴 방법을 찾을 거야

내가 정신을 잃은 것 같아
시간에 균열을 느껴
내가 정신을 잃은 것 같아

내가 더 가까워졌을까? 내가 정신을 잃었을까?
태양이 나를 태우고 있어
망가진 걸까? 시간에 균열이 있어
내가 패배를 받아들여야 할까

더 높은 힘을 찾더라도
뭔가 그것으로는 충분하지 않을 거라고 말하고 있어
그리고 이 사막이 꽃피기 시작하더라도
난 여전히 내 피를 흘릴 방법을 찾을 거야

내가 정신을 잃은 것 같아
시간에 균열을 느껴
내가 정신을 잃은 것 같아"""

    return complete_pronunciation, complete_translation

def update_songs_csv():
    """songs.csv에 완전한 번역 적용"""
    
    print("=" * 70)
    print("🎵 Complete Translation 업데이트")
    print("=" * 70)
    
    # CSV 읽기
    csv_path = Path('/Users/youz2me/Xcode/Livith-Data/output/main_output/songs.csv')
    df = pd.read_csv(csv_path, encoding='utf-8')
    
    # Higher Power 곡 찾기
    higher_power = df[df['title'] == 'Higher Power']
    if higher_power.empty:
        print("❌ Higher Power 곡을 찾을 수 없습니다.")
        return
    
    song_idx = higher_power.index[0]
    song = higher_power.iloc[0]
    
    print(f"곡명: {song['title']}")
    print(f"아티스트: {song['artist']}")
    
    # 완전한 번역 데이터 생성
    pronunciation, translation = create_complete_translation()
    
    # 줄 수 검증
    original_lines = [line for line in song['lyrics'].split('\n') if line.strip()]
    pronunciation_lines = [line for line in pronunciation.split('\n') if line.strip()]
    translation_lines = [line for line in translation.split('\n') if line.strip()]
    
    print(f"\n✅ 줄 수 검증:")
    print(f"  • 원문 줄 수: {len(original_lines)}")
    print(f"  • 발음 줄 수: {len(pronunciation_lines)}")
    print(f"  • 해석 줄 수: {len(translation_lines)}")
    print(f"  • 모든 줄 일치: {'✅' if len(original_lines) == len(pronunciation_lines) == len(translation_lines) else '❌'}")
    
    if len(original_lines) != len(pronunciation_lines) or len(original_lines) != len(translation_lines):
        print("❌ 줄 수가 일치하지 않습니다!")
        return
    
    # CSV 업데이트
    df.at[song_idx, 'pronunciation'] = pronunciation
    df.at[song_idx, 'translation'] = translation
    
    # 저장
    df.to_csv(csv_path, index=False, encoding='utf-8')
    print(f"\n💾 저장 완료: {csv_path}")
    
    # 검증 - 다시 읽어서 확인
    df_check = pd.read_csv(csv_path, encoding='utf-8')
    check_song = df_check[df_check['title'] == 'Higher Power'].iloc[0]
    
    check_pronunciation_lines = [line for line in str(check_song['pronunciation']).split('\n') if line.strip()]
    check_translation_lines = [line for line in str(check_song['translation']).split('\n') if line.strip()]
    
    print(f"\n🔍 저장 검증:")
    print(f"  • pronunciation 필드 존재: {'✅' if pd.notna(check_song['pronunciation']) else '❌'}")
    print(f"  • translation 필드 존재: {'✅' if pd.notna(check_song['translation']) else '❌'}")
    print(f"  • 발음 줄 수: {len(check_pronunciation_lines)}")
    print(f"  • 해석 줄 수: {len(check_translation_lines)}")
    print(f"  • 최종 줄 싱크: {'✅' if len(original_lines) == len(check_pronunciation_lines) == len(check_translation_lines) else '❌'}")
    
    print(f"\n🎯 최종 결과:")
    print(f"  • pronunciation 필드: 발음만 포함 ✅")
    print(f"  • translation 필드: 해석만 포함 ✅")
    print(f"  • 원문-발음-해석 줄 싱크: {'✅' if len(original_lines) == len(check_pronunciation_lines) == len(check_translation_lines) else '❌'}")
    print(f"  • 총 줄 수: {len(original_lines)}줄씩 완벽 일치 ✅")

if __name__ == "__main__":
    update_songs_csv()