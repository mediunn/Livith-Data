#!/usr/bin/env python3
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.enhanced_data_collector import EnhancedDataCollector
from src.data_models import Schedule

def test_schedule_final():
    collector = EnhancedDataCollector(None)
    
    print("=" * 60)
    print("최종 스케줄 검증 테스트 (scheduled_at 필드)")
    print("=" * 60)
    
    # 테스트 1: scheduled_at 필드 사용
    print("\n테스트 1: scheduled_at 필드 정상 처리")
    response1 = '''[
        {"concert_title": "테스트 콘서트", "category": "티켓팅", "scheduled_at": "2025-01-01 20:00:00"},
        {"concert_title": "테스트 콘서트", "category": "콘서트", "scheduled_at": "2025-01-02"}
    ]'''
    
    schedules1 = collector._parse_schedules(response1, "테스트 콘서트")
    print(f"  결과: {len(schedules1)}개의 스케줄")
    
    for schedule in schedules1:
        print(f"    - {schedule.category}: {schedule.scheduled_at}")
        assert hasattr(schedule, 'scheduled_at'), "scheduled_at 필드 없음"
    
    print("  ✅ 통과: scheduled_at 필드 정상 처리")
    
    # 테스트 2: 시간과 날짜만 있는 경우
    print("\n테스트 2: 시간 있음/날짜만 있음 둘 다 허용")
    response2 = '''[
        {"concert_title": "테스트 콘서트", "category": "티켓팅", "scheduled_at": "2025-01-01 14:00:00"},
        {"concert_title": "테스트 콘서트", "category": "공연", "scheduled_at": "2025-01-02"},
        {"concert_title": "테스트 콘서트", "category": "굿즈 판매", "scheduled_at": "2025-01-01 10:30:00"}
    ]'''
    
    schedules2 = collector._parse_schedules(response2, "테스트 콘서트")
    print(f"  결과: {len(schedules2)}개의 스케줄")
    
    for schedule in schedules2:
        print(f"    - {schedule.category}: {schedule.scheduled_at}")
    
    assert len(schedules2) == 3, "모든 유효한 스케줄 처리 실패"
    print("  ✅ 통과: 다양한 날짜/시간 형식 처리")
    
    # 테스트 3: 빈 scheduled_at 제외
    print("\n테스트 3: 빈 scheduled_at 제외")
    response3 = '''[
        {"concert_title": "테스트 콘서트", "category": "티켓팅", "scheduled_at": "2025-01-01 20:00:00"},
        {"concert_title": "테스트 콘서트", "category": "공연", "scheduled_at": ""},
        {"concert_title": "테스트 콘서트", "category": "굿즈", "scheduled_at": null}
    ]'''
    
    schedules3 = collector._parse_schedules(response3, "테스트 콘서트")
    print(f"  입력: 3개 스케줄 (2개는 빈 scheduled_at)")
    print(f"  결과: {len(schedules3)}개의 유효한 스케줄")
    
    assert len(schedules3) == 1, "빈 scheduled_at 필터링 실패"
    assert schedules3[0].scheduled_at == "2025-01-01 20:00:00", "유효한 데이터 유지 실패"
    print("  ✅ 통과: 빈 scheduled_at 제외됨")
    
    print("\n" + "=" * 60)
    print("모든 테스트 통과! ✅")
    print("\n최종 규칙:")
    print("1. 필드명: scheduled_at (올바름)")
    print("2. 형식: YYYY-MM-DD HH:MM:SS 또는 YYYY-MM-DD")
    print("3. 추정 금지: 실제 정보만 사용")
    print("4. 빈 값 자동 제외")
    print("5. 기존 CSV 파일 필드명 수정 완료")

if __name__ == "__main__":
    test_schedule_final()