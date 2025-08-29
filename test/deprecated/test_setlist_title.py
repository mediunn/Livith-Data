#!/usr/bin/env python3
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.enhanced_data_collector import EnhancedDataCollector
from src.data_models import Setlist, ConcertSetlist

def test_parse_concert_setlists():
    collector = EnhancedDataCollector(None)
    
    # 테스트 케이스 1: setlist_title이 비어있는 경우 (EXPECTED)
    response1 = '''[{"concert_title": "테스트 콘서트", "setlist_title": "", "type": "EXPECTED", "status": ""}]'''
    result1 = collector._parse_concert_setlists(response1, "테스트 콘서트", [])
    print("테스트 1 (EXPECTED, 빈 타이틀):")
    print(f"  입력: setlist_title=''")
    print(f"  결과: setlist_title='{result1[0].setlist_title}'")
    print(f"  예상: setlist_title='테스트 콘서트 예상 셋리스트'")
    assert result1[0].setlist_title == "테스트 콘서트 예상 셋리스트", "EXPECTED 타입 자동 채우기 실패"
    print("  ✓ 통과\n")
    
    # 테스트 케이스 2: setlist_title이 비어있는 경우 (PAST)
    response2 = '''[{"concert_title": "테스트 콘서트", "setlist_title": "", "type": "PAST", "status": ""}]'''
    result2 = collector._parse_concert_setlists(response2, "테스트 콘서트", [])
    print("테스트 2 (PAST, 빈 타이틀):")
    print(f"  입력: setlist_title=''")
    print(f"  결과: setlist_title='{result2[0].setlist_title}'")
    print(f"  예상: setlist_title='테스트 콘서트 셋리스트'")
    assert result2[0].setlist_title == "테스트 콘서트 셋리스트", "PAST 타입 자동 채우기 실패"
    print("  ✓ 통과\n")
    
    # 테스트 케이스 3: setlist_title이 이미 있는 경우
    response3 = '''[{"concert_title": "테스트 콘서트", "setlist_title": "커스텀 타이틀", "type": "EXPECTED", "status": ""}]'''
    result3 = collector._parse_concert_setlists(response3, "테스트 콘서트", [])
    print("테스트 3 (기존 타이틀 유지):")
    print(f"  입력: setlist_title='커스텀 타이틀'")
    print(f"  결과: setlist_title='{result3[0].setlist_title}'")
    print(f"  예상: setlist_title='커스텀 타이틀'")
    assert result3[0].setlist_title == "커스텀 타이틀", "기존 타이틀 유지 실패"
    print("  ✓ 통과\n")
    
    # 테스트 케이스 4: 파싱 실패 시 기본값
    response4 = "잘못된 응답"
    test_setlist = Setlist(
        title="테스트 셋리스트",
        start_date="2025-01-01",
        end_date="2025-01-01",
        img_url="",
        artist="테스트 아티스트",
        venue="테스트 장소"
    )
    result4 = collector._parse_concert_setlists(response4, "테스트 콘서트", [test_setlist])
    print("테스트 4 (파싱 실패 시 기본값):")
    print(f"  결과: setlist_title='{result4[0].setlist_title}'")
    print(f"  예상: setlist_title='테스트 콘서트 셋리스트'")
    assert result4[0].setlist_title == "테스트 콘서트 셋리스트", "기본값 설정 실패"
    print("  ✓ 통과\n")
    
    print("모든 테스트 통과! ✅")
    print("\n이제 앞으로 concert_setlists 데이터가 저장될 때 setlist_title이 자동으로 규칙에 따라 채워집니다:")
    print("- EXPECTED 타입: '콘서트명 예상 셋리스트'")
    print("- PAST/ONGOING 타입: '콘서트명 셋리스트'")

if __name__ == "__main__":
    test_parse_concert_setlists()