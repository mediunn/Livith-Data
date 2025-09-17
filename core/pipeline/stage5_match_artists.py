#!/usr/bin/env python3
"""
단계 5: 아티스트명 매칭 및 정리
독립 실행 가능한 스크립트
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.config import Config
from core.pipeline.stages import Stage5_MatchArtistNames

def main():
    try:
        # 환경변수 검증 (필수는 아님)
        try:
            Config.validate()
        except:
            pass  # 아티스트 매칭은 API가 필요 없음
        
        # 단계 5 실행
        # 테스트 모드는 False로 설정 (개별 실행 시 전체 처리)
        success = Stage5_MatchArtistNames.run(test_mode=False)
        
        if success:
            print("\n✅ 단계 5 완료: 아티스트명 매칭 완료")
            print("모든 데이터 수집 프로세스가 완료되었습니다!")
            print(f"📁 결과 확인: {Config.OUTPUT_DIR}/")
        else:
            print("\n❌ 단계 5 실패")
            
    except Exception as e:
        print(f"❌ 오류: {e}")

if __name__ == "__main__":
    main()