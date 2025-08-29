#!/usr/bin/env python3
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.enhanced_data_collector import EnhancedDataCollector
from src.data_models import Schedule

def test_schedule_validation():
    collector = EnhancedDataCollector(None)
    
    print("=" * 60)
    print("스케줄 데이터 검증 테스트")
    print("=" * 60)
    
    # 테스트 1: schedule_at이 비어있는 경우 필터링
    print("\n테스트 1: schedule_at이 비어있는 데이터 필터링")
    response1 = '''[
        {"concert_title": "테스트 콘서트", "category": "티켓팅", "schedule_at": "2025-01-01 20:00:00"},
        {"concert_title": "테스트 콘서트", "category": "공연", "schedule_at": ""},
        {"concert_title": "테스트 콘서트", "category": "굿즈 판매", "schedule_at": "   "},
        {"concert_title": "테스트 콘서트", "category": "콘서트", "schedule_at": "2025-01-02 19:00:00"}
    ]'''
    
    schedules = collector._parse_schedules(response1, "테스트 콘서트")
    print(f"  입력: 4개의 스케줄 (2개는 빈 schedule_at)")
    print(f"  결과: {len(schedules)}개의 유효한 스케줄")
    
    for schedule in schedules:
        print(f"    - {schedule.category}: {schedule.schedule_at}")
    
    assert len(schedules) == 2, "빈 schedule_at 필터링 실패"
    print("  ✅ 통과: 빈 schedule_at이 제거됨")
    
    # 테스트 2: scheduled_at을 schedule_at으로 변환
    print("\n테스트 2: scheduled_at을 schedule_at으로 자동 변환")
    response2 = '''[
        {"concert_title": "테스트 콘서트", "category": "티켓팅", "scheduled_at": "2025-01-01 20:00:00"},
        {"concert_title": "테스트 콘서트", "category": "공연", "scheduled_at": "2025-01-02 19:00:00"}
    ]'''
    
    schedules2 = collector._parse_schedules(response2, "테스트 콘서트")
    print(f"  입력: scheduled_at 필드 사용")
    print(f"  결과: {len(schedules2)}개의 스케줄 (schedule_at으로 변환)")
    
    for schedule in schedules2:
        print(f"    - {schedule.category}: {schedule.schedule_at}")
    
    assert len(schedules2) == 2, "scheduled_at 변환 실패"
    assert all(hasattr(s, 'schedule_at') for s in schedules2), "schedule_at 필드 없음"
    print("  ✅ 통과: scheduled_at이 schedule_at으로 변환됨")
    
    # 테스트 3: 모든 데이터가 비어있는 경우
    print("\n테스트 3: 모든 schedule_at이 비어있는 경우")
    response3 = '''[
        {"concert_title": "테스트 콘서트", "category": "티켓팅", "schedule_at": ""},
        {"concert_title": "테스트 콘서트", "category": "공연", "schedule_at": null}
    ]'''
    
    schedules3 = collector._parse_schedules(response3, "테스트 콘서트")
    print(f"  입력: 2개의 스케줄 (모두 빈 schedule_at)")
    print(f"  결과: {len(schedules3)}개의 스케줄")
    
    assert len(schedules3) == 0, "빈 데이터만 있을 때 처리 실패"
    print("  ✅ 통과: 모든 빈 데이터 제거됨")
    
    print("\n" + "=" * 60)
    print("모든 테스트 통과! ✅")
    print("\n업데이트된 규칙:")
    print("1. schedule_at이 비어있으면 자동 제외")
    print("2. scheduled_at 필드는 schedule_at으로 자동 변환")
    print("3. 프롬프트에서 schedule_at 필수 입력 강조")
    print("4. 기존 schedule.csv의 빈 데이터 제거 완료")

if __name__ == "__main__":
    test_schedule_validation()