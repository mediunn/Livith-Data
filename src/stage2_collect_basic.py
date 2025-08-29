#!/usr/bin/env python3
"""
단계 2: 기본 콘서트 정보 수집
독립 실행 가능한 스크립트
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import Config
from src.stages import Stage2_CollectBasicInfo

def main():
    try:
        # 환경변수 검증
        Config.validate()
        
        # 단계 2 실행 (이전 단계 결과를 CSV에서 로드)
        # 테스트 모드는 False로 설정 (개별 실행 시 전체 처리)
        all_collected_data = Stage2_CollectBasicInfo.run(test_mode=False)
        
        if all_collected_data:
            print(f"\n✅ 단계 2 완료: {len(all_collected_data)}개의 콘서트 기본 정보 수집")
            print("다음 단계: python src/stage3_collect_detailed.py")
        else:
            print("\n❌ 단계 2 실패")
            print("💡 단계 1을 먼저 실행했는지 확인하세요: python src/stage1_fetch_kopis.py")
            
    except ValueError as e:
        print(f"❌ 설정 오류: {e}")
        print("💡 .env 파일에 PERPLEXITY_API_KEY가 설정되어 있는지 확인하세요.")
    except Exception as e:
        print(f"❌ 오류: {e}")

if __name__ == "__main__":
    main()