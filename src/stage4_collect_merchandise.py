#!/usr/bin/env python3
"""
단계 4: 굿즈(MD) 정보 수집
독립 실행 가능한 스크립트
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import Config
from src.stages import Stage4_CollectMerchandise

def main():
    try:
        # 환경변수 검증
        Config.validate()
        
        # 단계 4 실행 (콘서트 정보에서 로드)
        # 테스트 모드는 False로 설정 (개별 실행 시 전체 처리)
        merchandise_data = Stage4_CollectMerchandise.run(test_mode=False)
        
        if merchandise_data:
            print(f"\n✅ 단계 4 완료: {len(merchandise_data)}개의 굿즈 정보 수집")
            print("생성된 파일: md.csv")
        else:
            print("\n⚪ 단계 4 완료: 굿즈 정보가 없거나 수집되지 않음")
        
        print("다음 단계: python src/stage5_match_artists.py")
            
    except ValueError as e:
        print(f"❌ 설정 오류: {e}")
        print("💡 .env 파일에 PERPLEXITY_API_KEY가 설정되어 있는지 확인하세요.")
    except Exception as e:
        print(f"❌ 오류: {e}")

if __name__ == "__main__":
    main()