"""
CSV 파일 안전 저장 유틸리티 (main_output에 있는 파일을 백업)
저장 전 기존 파일을 자동으로 백업
"""
import os
import pandas as pd
from lib.config import Config
import logging

logger = logging.getLogger(__name__)

class SafeWriter:

    @staticmethod
    def save_dataframe(df: pd.DataFrame, filename: str, backup_if_main: bool = True) -> str:
        # DataFrame을 CSV로 저장, 메인 출력 디렉토리일 경우 기존 파일 백업 후 저장
        filepath = os.path.join(Config.OUTPUT_DIR, filename)

        if backup_if_main and str(Config.OUTPUT_DIR) == str(Config.DATA_DIR / "main_output"):
            backup_path = SafeWriter._create_backup_if_needed(filename)
            if backup_path:
                logger.info(f"백업 생성: {os.path.basename(backup_path)}")

        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.info(f"파일 저장: {filepath}")

        return filepath

    @staticmethod
    def _create_backup_if_needed(filename: str) -> str:
        # 원본 파일이 존재할 때만 백업 생성
        original_path = os.path.join(Config.DATA_DIR / "main_output", filename)

        if os.path.exists(original_path):
            return Config.create_backup(filename)

        return None
