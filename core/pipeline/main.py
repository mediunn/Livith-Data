#!/usr/bin/env python3
"""
콘서트 데이터 수집 메인 파이프라인
"""
import sys
import os
import argparse
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from lib.config import Config
from core.pipeline.data_pipeline import DataPipeline

# 로깅 설정
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL, 'INFO'),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='콘서트 데이터 수집 파이프라인')
    
    parser.add_argument('--test', action='store_true', help='테스트 모드 (샘플 데이터만)')
    parser.add_argument('--full', action='store_true', help='전체 재수집 모드')
    parser.add_argument('--stage', type=int, choices=[1, 2, 3, 4, 5], help='특정 스테이지만 실행')
    
    args = parser.parse_args()
    
    try:
        # 설정 초기화
        Config.set_test_mode(args.test)
        Config.validate_api_keys()
        
        # 파이프라인 실행
        pipeline = DataPipeline()
        
        if args.stage:
            # 특정 스테이지만 실행
            success = pipeline.run_stage(args.stage, full_mode=args.full)
        else:
            # 전체 파이프라인 실행
            success = pipeline.run_full_pipeline(full_mode=args.full)
        
        if success:
            print("🎉 파이프라인 실행 완료!")
        else:
            print("❌ 파이프라인 실행 실패")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⚠️ 사용자가 중단했습니다.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"실행 오류: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()