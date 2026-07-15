import pandas as pd
import sys
import os
import logging
from tqdm import tqdm

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(project_root)

from lib.config import Config
from core.apis.gemini_api import GeminiAPI
from lib.data_collector import DataCollector

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def update_ticket_information(input_file: str, output_file: str):
    """
    기존 concerts.csv 파일을 읽어 티켓 정보를 업데이트합니다.
    """
    # 설정 및 API 클라이언트 초기화
    try:
        Config.validate_api_keys()
        api_client = GeminiAPI(api_key=Config.GEMINI_API_KEY)
        data_collector = DataCollector(api_client)
        logger.info("API 클라이언트 및 데이터 수집기 초기화 완료")
    except (ValueError, FileNotFoundError) as e:
        logger.error(f"초기화 실패: {e}")
        return

    # 데이터 읽기
    try:
        df = pd.read_csv(input_file)
        logger.info(f"'{input_file}'에서 {len(df)}개의 콘서트 데이터를 읽었습니다.")
    except FileNotFoundError:
        logger.error(f"입력 파일을 찾을 수 없습니다: '{input_file}'")
        return

    # 업데이트할 행 선택 (ticket_url이 비어있는 경우)
    to_update = df[df['ticket_url'].isnull() | (df['ticket_url'] == '')]
    
    if to_update.empty:
        logger.info("업데이트할 콘서트가 없습니다.")
        return

    logger.info(f"총 {len(to_update)}개의 콘서트 티켓 정보를 업데이트합니다.")

    # tqdm 설정
    pbar = tqdm(total=len(to_update), desc="티켓 정보 업데이트 중")

    # 각 콘서트에 대해 티켓 정보 수집
    for index, row in to_update.iterrows():
        title = row['title']
        artist = row['artist']
        start_date = row['start_date']
        
        try:
            logger.info(f"콘서트 정보 수집 중: {title} ({artist})")
            ticket_info = data_collector._collect_ticket_info(title, artist, start_date)
            
            if ticket_info:
                df.loc[index, 'ticket_url'] = ticket_info.get('url', '')
                df.loc[index, 'ticket_site'] = ticket_info.get('site', '')
                logger.info(f"  -> 티켓 정보 업데이트: URL='{ticket_info.get('url', '')}', Site='{ticket_info.get('site', '')}'")
            else:
                logger.warning(f"  -> 티켓 정보를 찾지 못했습니다.")

        except Exception as e:
            logger.error(f"'{title}' 정보 수집 중 오류 발생: {e}")
        
        pbar.update(1)

    pbar.close()

    # 업데이트된 데이터 저장
    try:
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"업데이트된 콘서트 데이터를 '{output_file}'에 저장했습니다.")
    except IOError as e:
        logger.error(f"파일 저장 실패: {e}")

if __name__ == "__main__":
    # 입출력 파일 경로 설정
    csv_file = Config.OUTPUT_DIR / "concerts.csv"
    
    # 백업 생성
    backup_path = Config.create_backup("concerts.csv")
    if backup_path:
        logger.info(f"'concerts.csv'를 '{backup_path}'에 백업했습니다.")
    else:
        logger.warning("'concerts.csv' 파일이 없어 백업을 생성하지 못했습니다.")

    # 티켓 정보 업데이트 실행 (기존 파일을 덮어쓰기)
    update_ticket_information(str(csv_file), str(csv_file))
    
    logger.info(f"'{csv_file}' 파일을 업데이트했습니다.")
