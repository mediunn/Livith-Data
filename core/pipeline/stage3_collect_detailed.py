#!/usr/bin/env python3
"""
단계 3: 상세 데이터 수집 (아티스트, 셋리스트, 곡, 문화 등)
독립 실행 가능한 스크립트
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.config import Config
from core.pipeline.stages import Stage3_CollectDetailedInfo

def main():
    try:
        # 환경변수 검증
        Config.validate()
        
        # 단계 3 실행 (이전 단계 결과를 필요시 재수집)
        # 테스트 모드는 False로 설정 (개별 실행 시 전체 처리)
        all_collected_data = Stage3_CollectDetailedInfo.run(test_mode=False)
        
        if all_collected_data:
            print(f"\n✅ 단계 3 완료: 상세 데이터 수집 완료")
            print("생성된 파일:")
            print("  - concerts.csv: 콘서트 상세 정보")
            print("  - artists.csv: 아티스트 정보")
            print("  - setlists.csv: 셋리스트 정보")
            print("  - songs.csv: 곡 정보")
            print("  - cultures.csv: 팬 문화 정보")
            print("다음 단계: python src/stage4_collect_merchandise.py")
        else:
            print("\n❌ 단계 3 실패")
            print("💡 단계 2를 먼저 실행했는지 확인하세요: python src/stage2_collect_basic.py")
            
    except ValueError as e:
        print(f"❌ 설정 오류: {e}")
        print("💡 .env 파일에 PERPLEXITY_API_KEY가 설정되어 있는지 확인하세요.")
    except Exception as e:
        print(f"❌ 오류: {e}")

if __name__ == "__main__":
    main()