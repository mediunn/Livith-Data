"""
안전한 파일 쓰기 유틸리티
메인 출력 디렉토리의 파일은 백업 후 편집
"""
import os
import pandas as pd
from utils.config import Config
import logging

logger = logging.getLogger(__name__)

class SafeWriter:
    """안전한 파일 쓰기 클래스"""
    
    @staticmethod
    def save_dataframe(df: pd.DataFrame, filename: str, backup_if_main: bool = True) -> str:
        """
        DataFrame을 CSV 파일로 안전하게 저장
        
        Args:
            df: 저장할 DataFrame
            filename: 파일명
            backup_if_main: 메인 출력 디렉토리일 때 백업 생성 여부
        
        Returns:
            저장된 파일의 전체 경로
        """
        filepath = os.path.join(Config.OUTPUT_DIR, filename)
        
        # 메인 출력 디렉토리이고 백업 옵션이 True인 경우
        if backup_if_main and Config.OUTPUT_DIR == Config.MAIN_OUTPUT_DIR:
            backup_path = SafeWriter._create_backup_if_needed(filename)
            if backup_path:
                logger.info(f"📋 백업 생성: {os.path.basename(backup_path)}")
        
        # DataFrame 저장
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.info(f"💾 파일 저장: {filepath}")
        
        return filepath
    
    @staticmethod
    def _create_backup_if_needed(filename: str) -> str:
        """필요한 경우 백업 파일 생성"""
        original_path = os.path.join(Config.MAIN_OUTPUT_DIR, filename)
        
        # 원본 파일이 존재하는 경우에만 백업
        if os.path.exists(original_path):
            return Config.create_backup(filename)
        
        return None
    
    @staticmethod
    def list_backups(filename: str) -> list:
        """특정 파일의 백업 목록 반환"""
        backups = Config.get_backup_files(filename)
        return [os.path.basename(backup) for backup in backups]
    
    @staticmethod
    def restore_from_backup(filename: str, backup_filename: str = None) -> bool:
        """
        백업에서 파일 복원
        
        Args:
            filename: 복원할 원본 파일명
            backup_filename: 백업 파일명 (None이면 최신 백업 사용)
        
        Returns:
            복원 성공 여부
        """
        try:
            backup_files = Config.get_backup_files(filename)
            
            if not backup_files:
                logger.error(f"백업 파일을 찾을 수 없습니다: {filename}")
                return False
            
            # 백업 파일 선택
            if backup_filename:
                backup_path = os.path.join(Config.BACKUP_DIR, backup_filename)
                if backup_path not in backup_files:
                    logger.error(f"지정한 백업 파일을 찾을 수 없습니다: {backup_filename}")
                    return False
            else:
                backup_path = backup_files[0]  # 최신 백업
            
            # 복원
            import shutil
            original_path = os.path.join(Config.MAIN_OUTPUT_DIR, filename)
            shutil.copy2(backup_path, original_path)
            
            logger.info(f"📋 백업에서 복원: {os.path.basename(backup_path)} -> {filename}")
            return True
            
        except Exception as e:
            logger.error(f"백업 복원 실패: {e}")
            return False
    
    @staticmethod
    def show_backup_status(filename: str = None):
        """백업 상태 표시"""
        if filename:
            # 특정 파일의 백업 상태
            backups = SafeWriter.list_backups(filename)
            print(f"\n📋 {filename} 백업 현황:")
            if backups:
                for i, backup in enumerate(backups, 1):
                    print(f"   {i}. {backup}")
            else:
                print("   백업이 없습니다.")
        else:
            # 전체 백업 상태
            import glob
            backup_pattern = os.path.join(Config.BACKUP_DIR, "*.csv")
            all_backups = glob.glob(backup_pattern)
            
            if all_backups:
                print(f"\n📋 전체 백업 현황 ({len(all_backups)}개):")
                # 파일별로 그룹화
                from collections import defaultdict
                backup_groups = defaultdict(list)
                
                for backup_path in all_backups:
                    backup_filename = os.path.basename(backup_path)
                    # 원본 파일명 추출 (timestamp 제거)
                    parts = backup_filename.split('_')
                    if len(parts) >= 3:  # name_YYYYMMDD_HHMMSS.ext
                        original_name = '_'.join(parts[:-2]) + '.csv'
                        backup_groups[original_name].append(backup_filename)
                
                for original_name, backups in backup_groups.items():
                    print(f"   📄 {original_name}: {len(backups)}개 백업")
            else:
                print("\n📋 백업이 없습니다.")