#!/usr/bin/env python3
"""
콘서트 상태 업데이트 스크립트 (Windows 지원)
1. DB → CSV 다운로드 (백업 포함)
2. CSV에서 start_date / end_date 비교 후 status 갱신
3. 갱신된 CSV 내용을 DB에 UPDATE 반영
"""

import os
import sys
import pandas as pd
from datetime import datetime
import logging

# 프로젝트 루트 경로 추가 (lib 경로 사용 가능하게)
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from lib.db_utils import get_db_manager
from lib.config import Config


logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL, 'INFO'),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ConcertStatusUpdater:
    def __init__(self):
        self.today_date = datetime.now().date()
        self.csv_file = os.path.join(Config.OUTPUT_DIR, "concerts.csv")

    def download_table(self):
        """MySQL → CSV 다운로드"""
        db = get_db_manager()
        if not db.connect_with_ssh():
            return False

        try:
            db.cursor = db.connection.cursor(dictionary=True)
            db.cursor.execute("SELECT * FROM concerts")
            data = db.cursor.fetchall()
            if not data:
                logger.warning("⚠️ concerts 테이블이 비어있습니다.")
                return False

            df = pd.DataFrame(data)

            # 기존 CSV 백업
            if os.path.exists(self.csv_file):
                backup_file = f"concerts_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                backup_path = os.path.join(Config.BACKUP_DIR, backup_file)
                pd.read_csv(self.csv_file, encoding="utf-8-sig").to_csv(backup_path, index=False, encoding="utf-8-sig")
                logger.info(f"💾 백업 생성: {backup_file}")

            df.to_csv(self.csv_file, index=False, encoding="utf-8-sig")
            logger.info(f"📁 concerts.csv 저장 완료 ({len(df)}개 레코드)")
            return True

        except Exception as e:
            logger.error(f"❌ 다운로드 실패: {e}")
            return False
        finally:
            db.disconnect()

    def update_status_in_csv(self):
        """CSV에서 상태 업데이트"""
        if not os.path.exists(self.csv_file):
            logger.error("❌ concerts.csv 파일 없음. 먼저 다운로드하세요.")
            return 0

        df = pd.read_csv(self.csv_file, encoding="utf-8-sig")
        if "start_date" not in df.columns or "end_date" not in df.columns:
            logger.error("❌ start_date, end_date 컬럼 없음.")
            return 0

        updated_count = 0
        for idx, row in df.iterrows():
            new_status = self._calculate_status(row["start_date"], row["end_date"])
            if row.get("status", "") != new_status:
                df.at[idx, "status"] = new_status
                updated_count += 1

        if updated_count > 0:
            df.to_csv(self.csv_file, index=False, encoding="utf-8-sig")
            logger.info(f"✅ CSV 상태 업데이트 완료 ({updated_count}개)")
        else:
            logger.info("⚪ 업데이트할 상태 없음")

        return updated_count

    def apply_updates_to_db(self):
        """CSV → DB 반영"""
        if not os.path.exists(self.csv_file):
            logger.error("❌ concerts.csv 파일 없음")
            return False

        df = pd.read_csv(self.csv_file, encoding="utf-8-sig")

        db = get_db_manager()
        if not db.connect_with_ssh():
            return False

        try:
            cursor = db.connection.cursor()
            updated_rows = 0

            for _, row in df.iterrows():
                query = """
                    UPDATE concerts
                    SET status = %s, updated_at = NOW()
                    WHERE id = %s
                """
                cursor.execute(query, (row["status"], row["id"]))
                updated_rows += 1

            db.connection.commit()
            logger.info(f"🎉 DB 반영 완료 ({updated_rows}개 레코드)")
            return True

        except Exception as e:
            logger.error(f"❌ DB 업데이트 실패: {e}")
            return False
        finally:
            db.disconnect()

    def _calculate_status(self, start_date, end_date):
        """날짜 기반 상태 계산"""
        try:
            start = self._parse_date(start_date)
            end = self._parse_date(end_date)
            if not start or not end:
                return "UNKNOWN"

            if self.today_date < start:
                return "UPCOMING"
            elif start <= self.today_date <= end:
                return "ONGOING"
            else:
                return "COMPLETED"
        except:
            return "UNKNOWN"

    def _parse_date(self, date_str):
        """다양한 날짜 형식 파싱"""
        if pd.isna(date_str) or not date_str:
            return None
        s = str(date_str).strip()

        for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y.%m.%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except:
                continue
        return None


def main():
    updater = ConcertStatusUpdater()
    print("🚀 콘서트 상태 업데이트 시작")

    if updater.download_table():
        updater.update_status_in_csv()
        updater.apply_updates_to_db()

    print("✅ 전체 프로세스 완료!")


if __name__ == "__main__":
    main()
