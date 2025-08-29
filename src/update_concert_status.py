#!/usr/bin/env python3
"""
콘서트 상태 업데이트 스크립트
매일 실행하여 콘서트 상태를 자동으로 업데이트합니다.
"""
import sys
import os
import pandas as pd
from datetime import datetime
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import Config

logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL, 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ConcertStatusUpdater:
    """콘서트 상태 업데이트 클래스"""
    
    def __init__(self):
        self.today = datetime.now().strftime("%Y%m%d")
        self.today_date = datetime.now().date()
    
    def update_all_concerts(self):
        """모든 콘서트 파일의 상태를 업데이트"""
        updated_files = []
        
        # 업데이트할 CSV 파일들
        csv_files = [
            'concerts.csv',
            'kopis_filtered_concerts.csv', 
            'step1_basic_concerts.csv'
        ]
        
        for filename in csv_files:
            filepath = os.path.join(Config.OUTPUT_DIR, filename)
            if os.path.exists(filepath):
                updated_count = self._update_csv_file(filepath)
                if updated_count > 0:
                    updated_files.append((filename, updated_count))
                    logger.info(f"✅ {filename}: {updated_count}개 콘서트 상태 업데이트")
                else:
                    logger.info(f"⚪ {filename}: 업데이트할 콘서트 없음")
            else:
                logger.warning(f"❌ {filename}: 파일이 존재하지 않음")
        
        return updated_files
    
    def _update_csv_file(self, filepath: str) -> int:
        """개별 CSV 파일의 상태를 업데이트"""
        try:
            # CSV 파일 읽기
            df = pd.read_csv(filepath, encoding='utf-8-sig')
            
            if 'start_date' not in df.columns or 'end_date' not in df.columns:
                logger.warning(f"날짜 컬럼이 없어 상태 업데이트 불가: {filepath}")
                return 0
            
            updated_count = 0
            original_status_col = 'status' if 'status' in df.columns else None
            
            # 새로운 상태 계산
            for idx, row in df.iterrows():
                new_status = self._calculate_status(row['start_date'], row['end_date'])
                old_status = row.get('status', '') if original_status_col else ''
                
                # 상태가 변경된 경우만 업데이트
                if new_status != old_status:
                    df.at[idx, 'status'] = new_status
                    updated_count += 1
                    
                    # 로그 출력
                    title = row.get('title', row.get('prfnm', '알 수 없는 콘서트'))
                    logger.debug(f"상태 변경: {title} [{old_status}] -> [{new_status}]")
            
            # 변경사항이 있으면 파일 저장
            if updated_count > 0:
                df.to_csv(filepath, index=False, encoding='utf-8-sig')
            
            return updated_count
            
        except Exception as e:
            logger.error(f"CSV 파일 업데이트 실패: {filepath} - {e}")
            return 0
    
    def _calculate_status(self, start_date: str, end_date: str) -> str:
        """날짜를 기반으로 콘서트 상태 계산"""
        try:
            # 날짜 형식 파싱 (YYYYMMDD 또는 YYYY.MM.DD 또는 YYYY-MM-DD)
            start = self._parse_date(start_date)
            end = self._parse_date(end_date)
            
            if not start or not end:
                return 'UNKNOWN'
            
            # 상태 결정
            if self.today_date < start:
                return 'UPCOMING'  # 공연 예정
            elif start <= self.today_date <= end:
                return 'ONGOING'   # 공연 중
            else:
                return 'COMPLETED' # 공연 완료
                
        except Exception as e:
            logger.error(f"날짜 파싱 실패: {start_date}, {end_date} - {e}")
            return 'UNKNOWN'
    
    def _parse_date(self, date_str: str):
        """다양한 형식의 날짜를 파싱"""
        if not date_str or pd.isna(date_str):
            return None
        
        date_str = str(date_str).strip()
        
        # YYYYMMDD 형식
        if len(date_str) == 8 and date_str.isdigit():
            try:
                return datetime.strptime(date_str, '%Y%m%d').date()
            except:
                pass
        
        # YYYY.MM.DD 형식
        if '.' in date_str:
            try:
                return datetime.strptime(date_str, '%Y.%m.%d').date()
            except:
                pass
        
        # YYYY-MM-DD 형식
        if '-' in date_str:
            try:
                return datetime.strptime(date_str, '%Y-%m-%d').date()
            except:
                pass
        
        return None
    
    def show_status_summary(self):
        """현재 상태 요약 표시"""
        concerts_file = os.path.join(Config.OUTPUT_DIR, 'concerts.csv')
        
        if not os.path.exists(concerts_file):
            print("❌ concerts.csv 파일을 찾을 수 없습니다.")
            return
        
        try:
            df = pd.read_csv(concerts_file, encoding='utf-8-sig')
            
            if 'status' not in df.columns:
                print("❌ 상태 컬럼이 없습니다.")
                return
            
            # 상태별 집계
            status_counts = df['status'].value_counts()
            
            print("\n" + "=" * 50)
            print("📊 콘서트 상태 요약")
            print("=" * 50)
            print(f"📅 기준일: {self.today_date}")
            print()
            
            status_names = {
                'UPCOMING': '🟡 공연 예정',
                'ONGOING': '🔴 공연 중',
                'COMPLETED': '🟢 공연 완료',
                'UNKNOWN': '⚪ 상태 불명'
            }
            
            for status, count in status_counts.items():
                status_name = status_names.get(status, f"❓ {status}")
                print(f"   {status_name}: {count}개")
            
            print(f"\n총 {len(df)}개 콘서트")
            print("=" * 50)
            
        except Exception as e:
            logger.error(f"상태 요약 표시 실패: {e}")

def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='콘서트 상태 업데이트 도구',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  python src/update_concert_status.py           # 모든 콘서트 상태 업데이트
  python src/update_concert_status.py --summary # 현재 상태 요약만 표시
  python src/update_concert_status.py --dry-run # 변경사항 미리보기

상태 설명:
  UPCOMING  - 공연 예정 (시작일이 오늘 이후)
  ONGOING   - 공연 중 (시작일 <= 오늘 <= 종료일)
  COMPLETED - 공연 완료 (종료일이 오늘 이전)
  UNKNOWN   - 날짜 정보 부족으로 상태 불명
        """
    )
    
    parser.add_argument(
        '--summary', 
        action='store_true',
        help='현재 상태 요약만 표시 (업데이트 안함)'
    )
    parser.add_argument(
        '--dry-run', 
        action='store_true',
        help='변경사항 미리보기 (실제 업데이트 안함)'
    )
    
    args = parser.parse_args()
    
    updater = ConcertStatusUpdater()
    
    try:
        if args.summary:
            # 상태 요약만 표시
            updater.show_status_summary()
        else:
            # 상태 업데이트 실행
            print("🔄 콘서트 상태 업데이트 시작...")
            print(f"📅 기준일: {updater.today_date}")
            print()
            
            if args.dry_run:
                print("⚠️  DRY RUN 모드: 실제로 파일을 변경하지 않습니다.")
                # TODO: dry-run 모드 구현
            
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
            
    except KeyboardInterrupt:
        print("\n⚠️  작업이 사용자에 의해 중단되었습니다.")
        
    except Exception as e:
        logger.error(f"예상치 못한 오류 발생: {e}")
        print(f"❌ 오류: {e}")

if __name__ == "__main__":
    main()