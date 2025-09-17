#!/usr/bin/env python3
"""
단계 1: KOPIS API에서 공연 데이터 수집 및 필터링
독립 실행 가능한 스크립트
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.config import Config
from core.pipeline.stages import Stage1_FetchKopisData

def main():
    try:
        # 환경변수 검증
        Config.validate()
        
        # 테스트 모드 선택
        print("\n" + "=" * 60)
        print("🎵 KOPIS 데이터 수집")
        print("=" * 60)
        print("\n실행 모드를 선택하세요:")
        print("1. 테스트 모드 (내한공연 1개만 처리)")
        print("2. 전체 모드 (모든 내한공연 처리)")
        
        test_mode = None
        while True:
            try:
                choice = input("\n선택 (1 또는 2): ")
                if choice == '1':
                    test_mode = True
                    print("\n⚠️  테스트 모드로 실행합니다.")
                    break
                elif choice == '2':
                    test_mode = False
                    print("\n📋 전체 모드로 실행합니다.")
                    break
                else:
                    print("❌ 1 또는 2를 입력하세요.")
            except KeyboardInterrupt:
                print("\n❌ 취소되었습니다.")
                return
        
        # 단계 1 실행
        concert_details = Stage1_FetchKopisData.run(test_mode=test_mode)
        
        if concert_details:
            print(f"\n✅ 단계 1 완료: {len(concert_details)}개의 내한 콘서트 처리")
            print("다음 단계: python src/stage2_collect_basic.py")
        else:
            print("\n❌ 단계 1 실패")
            
    except ValueError as e:
        print(f"❌ 설정 오류: {e}")
        print("💡 .env 파일에 KOPIS_API_KEY가 설정되어 있는지 확인하세요.")
    except Exception as e:
        print(f"❌ 오류: {e}")

if __name__ == "__main__":
    main()