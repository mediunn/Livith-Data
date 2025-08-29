#!/usr/bin/env python3
"""
내한 콘서트 데이터 수집 통합 실행 파일
모든 단계를 순차적으로 실행하거나 특정 단계만 실행 가능
"""
import sys
import os
import argparse
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import Config
from src.stages import (
    Stage1_FetchKopisData,
    Stage2_CollectBasicInfo, 
    Stage3_CollectDetailedInfo,
    Stage4_CollectMerchandise,
    Stage5_MatchArtistNames,
    StageRunner
)
from src.update_concert_status import ConcertStatusUpdater

logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL, 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(
        description='내한 콘서트 데이터 수집기',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  python src/main.py              # 모든 단계 실행 (모드 선택)
  python src/main.py --test       # 테스트 모드 (1개 콘서트만)
  python src/main.py --test --reset  # 테스트 모드 (데이터 초기화 후 실행)
  python src/main.py --full       # 전체 모드 (모든 콘서트)
  python src/main.py --mode full  # 전체 갱신 모드
  
  python src/main.py --stage 1    # 단계 1만 실행 (KOPIS 데이터 수집)
  python src/main.py --stage 1 --test  # 단계 1 테스트 모드
  python src/main.py --from 2     # 단계 2부터 끝까지 실행
  python src/main.py --from 3 --to 4 --full  # 단계 3-4 전체 모드
  
  python src/main.py --update-status  # 콘서트 상태만 업데이트

단계 설명:
  1: KOPIS API에서 공연 데이터 수집 및 필터링
  2: 기본 콘서트 정보 수집 (Perplexity API)
  3: 상세 데이터 수집 (아티스트, 셋리스트, 곡, 문화)
  4: 굿즈(MD) 정보 수집
  5: 아티스트명 매칭 및 정리
        """
    )
    
    parser.add_argument(
        '--stage', 
        type=int, 
        choices=[1, 2, 3, 4, 5],
        help='실행할 특정 단계 번호'
    )
    parser.add_argument(
        '--from', 
        type=int, 
        dest='from_stage',
        choices=[1, 2, 3, 4, 5],
        help='시작 단계 번호 (이 단계부터 끝까지 실행)'
    )
    parser.add_argument(
        '--to', 
        type=int, 
        dest='to_stage',
        choices=[1, 2, 3, 4, 5],
        help='종료 단계 번호 (--from과 함께 사용)'
    )
    parser.add_argument(
        '--mode',
        type=str,
        choices=['incremental', 'full'],
        default='incremental',
        help='데이터 수집 모드: incremental(증분, 기본값) 또는 full(전체 갱신)'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='테스트 모드 실행 (1개 콘서트만 처리)'
    )
    parser.add_argument(
        '--full',
        action='store_true',
        help='전체 모드 실행 (모든 콘서트 처리)'
    )
    parser.add_argument(
        '--update-status',
        action='store_true',
        help='콘서트 상태만 업데이트 (매일 실행 권장)'
    )
    parser.add_argument(
        '--reset',
        action='store_true',
        help='테스트 데이터 초기화 후 실행 (--test와 함께 사용)'
    )
    
    args = parser.parse_args()
    
    # 테스트 모드 결정
    test_mode = None
    reset_data = args.reset
    
    if args.test:
        test_mode = True
    elif args.full:
        test_mode = False
    
    # --reset 옵션은 --test와 함께 사용해야 함
    if reset_data and not args.test:
        print("❌ --reset 옵션은 --test와 함께 사용해야 합니다.")
        return
    
    try:
        # 상태 업데이트만 실행하는 경우
        if args.update_status:
            run_status_update()
            return
        
        # 환경변수 검증
        try:
            Config.validate()
        except ValueError as e:
            # 단계 5는 API 키가 필요 없음
            if args.stage == 5 or (args.from_stage == 5 and args.to_stage == 5):
                pass
            else:
                raise e
        
        # 단계 실행 로직
        if args.stage:
            # 특정 단계만 실행
            run_single_stage(args.stage, args.mode, test_mode)
        elif args.from_stage:
            # 범위 실행
            to_stage = args.to_stage if args.to_stage else 5
            if args.from_stage > to_stage:
                print("❌ 오류: --from 값이 --to 값보다 큽니다.")
                return
            run_stages_range(args.from_stage, to_stage, args.mode, test_mode)
        else:
            # 모든 단계 실행
            run_all_stages(args.mode, test_mode, reset_data)
            
    except ValueError as e:
        logger.error(f"설정 오류: {e}")
        print("❌ 환경변수 설정 오류")
        print("=" * 50)
        print(f"오류: {e}")
        print("\n💡 해결 방법:")
        print("1. .env 파일이 프로젝트 루트에 있는지 확인")
        print("2. .env 파일에 다음 내용이 설정되어 있는지 확인:")
        print("   PERPLEXITY_API_KEY=your_perplexity_api_key")
        print("   KOPIS_API_KEY=your_kopis_api_key")
        
    except KeyboardInterrupt:
        print("\n⚠️  작업이 사용자에 의해 중단되었습니다.")
        
    except Exception as e:
        logger.error(f"예상치 못한 오류 발생: {e}")
        print(f"❌ 예상치 못한 오류: {e}")

def run_single_stage(stage_num, mode='incremental', test_mode=None):
    """특정 단계만 실행"""
    mode_text = "증분 수집" if mode == 'incremental' else "전체 갱신"
    test_text = " (테스트)" if test_mode else " (전체)" if test_mode is not None else ""
    print(f"🎯 단계 {stage_num}만 실행합니다 ({mode_text}{test_text})")
    print("=" * 60)
    
    # 단계 1은 항상 테스트 모드 선택 가능
    if stage_num == 1 and test_mode is None:
        test_mode = None  # Stage1이 자체적으로 선택하도록
    
    stages = {
        1: lambda: Stage1_FetchKopisData.run(mode, test_mode),
        2: lambda: Stage2_CollectBasicInfo.run(None, mode, test_mode if test_mode is not None else False),
        3: lambda: Stage3_CollectDetailedInfo.run(None, mode, test_mode if test_mode is not None else False),
        4: lambda: Stage4_CollectMerchandise.run(None, mode, test_mode if test_mode is not None else False),
        5: lambda: Stage5_MatchArtistNames.run(test_mode if test_mode is not None else False)
    }
    
    if stage_num in stages:
        result = stages[stage_num]()
        if result or stage_num == 5:  # 단계 5는 boolean 반환
            print(f"\n✅ 단계 {stage_num} 완료!")
        else:
            print(f"\n⚠️  단계 {stage_num} 실행 중 문제가 발생했습니다.")

def run_stages_range(from_stage, to_stage, mode='incremental', test_mode=None):
    """지정된 범위의 단계 실행"""
    mode_text = "증분 수집" if mode == 'incremental' else "전체 갱신"
    test_text = " (테스트)" if test_mode else " (전체)" if test_mode is not None else ""
    print(f"🎯 단계 {from_stage}부터 {to_stage}까지 실행합니다 ({mode_text}{test_text})")
    print("=" * 60)
    
    # 이전 단계 결과를 전달하기 위한 변수
    concert_details = None
    all_collected_data = None
    
    for stage_num in range(from_stage, to_stage + 1):
        print(f"\n📍 단계 {stage_num} 실행 중...")
        
        if stage_num == 1:
            concert_details = Stage1_FetchKopisData.run(mode, test_mode)
            if not concert_details:
                print("❌ 단계 1 실패로 중단")
                break
                
        elif stage_num == 2:
            all_collected_data = Stage2_CollectBasicInfo.run(concert_details, mode, test_mode if test_mode is not None else False)
            if not all_collected_data:
                print("❌ 단계 2 실패로 중단")
                break
                
        elif stage_num == 3:
            Stage3_CollectDetailedInfo.run(all_collected_data, mode, test_mode if test_mode is not None else False)
            
        elif stage_num == 4:
            Stage4_CollectMerchandise.run(all_collected_data, mode, test_mode if test_mode is not None else False)
            
        elif stage_num == 5:
            Stage5_MatchArtistNames.run(test_mode if test_mode is not None else False)
    
    print(f"\n✅ 단계 {from_stage}~{to_stage} 완료!")

def run_all_stages(mode='incremental', test_mode=None, force_reset=False):
    """모든 단계 실행"""
    success = StageRunner.run_all(mode, test_mode, force_reset)
    if success:
        print("\n🎉 모든 데이터 수집 완료!")
    else:
        print("\n⚠️  일부 단계에서 문제가 발생했습니다.")

def run_status_update():
    """콘서트 상태 업데이트 실행"""
    print("🔄 콘서트 상태 업데이트 시작...")
    
    updater = ConcertStatusUpdater()
    
    try:
        updated_files = updater.update_all_concerts()
        
        print()
        if updated_files:
            print("✅ 상태 업데이트 완료!")
            for filename, count in updated_files:
                print(f"   📁 {filename}: {count}개 업데이트")
        else:
            print("⚪ 업데이트할 콘서트가 없습니다.")
        
        print()
        updater.show_status_summary()
        
    except Exception as e:
        logger.error(f"상태 업데이트 실패: {e}")
        print(f"❌ 상태 업데이트 오류: {e}")

if __name__ == "__main__":
    main()
