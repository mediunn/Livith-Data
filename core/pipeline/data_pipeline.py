"""
데이터 수집 파이프라인 핵심 로직
"""
import csv
import logging
import shutil
from datetime import datetime
from pathlib import Path
import pandas as pd
from typing import Optional, Tuple, List, Dict

from lib.config import Config
from lib.data_collector import DataCollector
from lib.safe_writer import SafeWriter
from core.apis.kopis_api import KopisAPI

KOPIS_BASE_DIR = Path("data/kopis_crawling")

# API 클라이언트 선택
if Config.USE_GEMINI_API:
    try:
        from core.apis.gemini_api import GeminiAPI as APIClient
    except ImportError:
        from core.apis.perplexity_api import PerplexityAPI as APIClient
else:
    from core.apis.perplexity_api import PerplexityAPI as APIClient

logger = logging.getLogger(__name__)


class DataPipeline:
    """단순화된 데이터 수집 파이프라인"""
    
    def __init__(self, start_date: str = None, end_date: str = None, concert_codes: List[str] = None):
        self.kopis_api = KopisAPI(Config.KOPIS_API_KEY)
        self.api_client = APIClient(Config.GEMINI_API_KEY if Config.USE_GEMINI_API else Config.PERPLEXITY_API_KEY)
        self.data_collector = DataCollector(self.api_client)
        self.writer = SafeWriter
        self.start_date = start_date
        self.end_date = end_date
        self.concert_codes = concert_codes
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.run_dir = KOPIS_BASE_DIR / timestamp / "db"
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.stats = {"concerts": 0, "artists": 0, "concert_list": [], "artist_list": []}
    
    def run_full_pipeline(self) -> bool:
        """전체 파이프라인 실행"""
        print("🚀 데이터 수집 파이프라인 시작")

        try:
            # 1단계: KOPIS 데이터 수집
            if self.concert_codes:
                # 직접 지정된 코드가 있으면 해당 코드들만 처리
                kopis_data = self._fetch_kopis_data_by_codes(self.concert_codes)
            else:
                # 기존 방식: 날짜 범위로 수집
                kopis_data = self._fetch_kopis_data()
            
            if not kopis_data:
                return False
            
            # 2단계: 콘서트 데이터 보강
            concerts = self._enhance_concert_data(kopis_data)
            
            # 3단계: 아티스트 데이터 수집 및 이름 통일
            artists, name_map = self._collect_artist_data(concerts)
            
            print("🔄 아티스트 이름 통일 중...")
            for concert in concerts:
                if concert.artist in name_map:
                    concert.artist = name_map[concert.artist]
            print("✅ 아티스트 이름 통일 완료")

            # 4단계: CSV 저장
            self._save_data(concerts, artists)
            
            print("✅ 파이프라인 완료")
            return True
            
        except Exception as e:
            logger.error(f"파이프라인 실행 실패: {e}")
            return False
    
    def run_stage(self, stage_num: int) -> bool:
        """특정 스테이지만 실행"""
        print(f"🎯 스테이지 {stage_num} 실행")

        try:
            if stage_num == 1:
                if self.concert_codes:
                    return self._fetch_kopis_data_by_codes(self.concert_codes) is not None
                return self._fetch_kopis_data() is not None
            elif stage_num == 2:
                # 기존 데이터 로드 후 보강
                return self._enhance_existing_data()
            elif stage_num == 3:
                return self._collect_artist_data_only()
            else:
                print(f"⚠️ 스테이지 {stage_num}는 지원되지 않습니다.")
                return False
                
        except Exception as e:
            logger.error(f"스테이지 {stage_num} 실행 실패: {e}")
            return False
    
    def _fetch_kopis_data_by_codes(self, codes: List[str]) -> Optional[list]:
        """특정 공연 코드들의 상세 정보 수집"""
        print(f"📡 지정된 공연 코드 {len(codes)}개 수집 중...")
        
        try:
            concerts = self.kopis_api.fetch_concert_details(
                codes, 
                max_found=None,
                skip_filter=True  # 내한공연 필터링 스킵 (이미 필터링된 코드들)
            )
            
            print(f"📊 수집 결과: {len(concerts)}개 공연 정보 수집")
            return concerts
            
        except Exception as e:
            logger.error(f"공연 코드 데이터 수집 실패: {e}")
            return None
    
    def _fetch_kopis_data(self) -> Optional[list]:
        """KOPIS API에서 데이터 수집"""
        print("📡 KOPIS 데이터 수집 중...")
        
        try:
            # 전체 공연 코드 수집
            concert_codes = self.kopis_api.fetch_all_concerts(self.start_date, self.end_date)
            print(f"📊 총 {len(concert_codes)}개 공연 코드 수집")
            
            concerts = self.kopis_api.fetch_concert_details(concert_codes)
            
            print(f"📊 수집 결과: {len(concerts)}개 내한공연 발견")
            return concerts
            
        except Exception as e:
            logger.error(f"KOPIS 데이터 수집 실패: {e}")
            return None
    
    def _enhance_concert_data(self, kopis_data: list) -> list:
        """콘서트 데이터 보강"""
        print("🔍 콘서트 정보 보강 중...")
        
        enhanced_concerts = []
        
        for i, kopis_concert in enumerate(kopis_data, 1):
            title = kopis_concert.get('title', 'Unknown')
            artist = kopis_concert.get('artist', '')
            try:
                print(f"  [{i}/{len(kopis_data)}] {title}")

                # 2차 Gemini 장르 필터 (1차 키워드 통과 후)
                if self.data_collector.is_excluded_genre(title, artist):
                    print(f"    → 2차 장르 필터 제외: {title}")
                    continue

                # 기본 콘서트 객체 생성
                concert = self.data_collector.collect_concert_basic_info(kopis_concert)

                # AI API로 정보 보강
                enhanced_concert = self.data_collector.enhance_concert_data(concert)

                enhanced_concerts.append(enhanced_concert)

            except Exception as e:
                logger.warning(f"콘서트 정보 보강 실패 ({title}): {e}")
                continue
        
        print(f"✅ {len(enhanced_concerts)}개 콘서트 정보 보강 완료")
        return enhanced_concerts
    
    def _collect_artist_data(self, concerts: list) -> Tuple[List, Dict]:
        """아티스트 데이터 수집 및 이름 변환 맵 반환"""
        print("🎤 아티스트 정보 수집 중...")
        
        # 고유 아티스트 추출
        unique_artists = set()
        for concert in concerts:
            if concert.artist:
                unique_artists.add(concert.artist)
        
        artists = []
        name_map = {}
        for i, artist_name in enumerate(unique_artists, 1):
            try:
                print(f"  [{i}/{len(unique_artists)}] {artist_name}")
                
                artist_obj = self.data_collector.collect_artist_info(artist_name)
                if artist_obj:
                    artists.append(artist_obj)
                    # 이름이 변경된 경우, 변환 맵에 추가
                    if artist_obj.artist and artist_obj.artist != artist_name:
                        name_map[artist_name] = artist_obj.artist
                        
            except Exception as e:
                logger.warning(f"아티스트 정보 수집 실패 ({artist_name}): {e}")
                continue
        
        print(f"✅ {len(artists)}명 아티스트 정보 수집 완료")
        return artists, name_map
    
    def _save_data(self, concerts: list, artists: list):
        """데이터 저장"""
        print("💾 데이터 저장 중...")

        try:
            concerts_df = pd.DataFrame([concert.__dict__ for concert in concerts])
            artists_df = pd.DataFrame([artist.__dict__ for artist in artists])

            self.writer.save_dataframe(concerts_df, "concerts.csv", backup_if_main=False)
            self.writer.save_dataframe(artists_df, "artists.csv", backup_if_main=False)

            # 타임스탬프 폴더에도 백업
            concerts_df.to_csv(self.run_dir / "concerts.csv", index=False, encoding="utf-8-sig")
            artists_df.to_csv(self.run_dir / "artists.csv", index=False, encoding="utf-8-sig")
            print(f"  → {self.run_dir} (concerts, artists)")

            self.stats["concerts"] = len(concerts_df)
            self.stats["artists"] = len(artists_df)
            if "title" in concerts_df.columns and "artist" in concerts_df.columns:
                self.stats["concert_list"] = [
                    f"{row['title']} — {row['artist']}" for _, row in concerts_df.iterrows()
                ]
            if "artist" in artists_df.columns:
                self.stats["artist_list"] = artists_df["artist"].tolist()
            print(f"✅ 데이터 저장 완료: {Config.OUTPUT_DIR}")

        except Exception as e:
            logger.error(f"데이터 저장 실패: {e}")
            raise

    def copy_to_run_dir(self, *filenames: str):
        """main_output의 CSV를 타임스탬프 폴더에 복사 (auto 파이프라인 후 호출)"""
        for filename in filenames:
            src = Config.OUTPUT_DIR / filename
            if src.exists():
                shutil.copy2(src, self.run_dir / filename)
                print(f"  → {self.run_dir / filename}")
    
    def _enhance_existing_data(self) -> bool:
        """기존 데이터 보강"""
        print("🔄 기존 데이터 보강 중...")
        
        try:
            # 기존 콘서트 데이터 로드
            concerts_path = Config.OUTPUT_DIR / "concerts.csv"
            if not concerts_path.exists():
                print("❌ 기존 콘서트 데이터가 없습니다.")
                return False
            
            df = pd.read_csv(concerts_path)
            print(f"📊 기존 데이터: {len(df)}개 콘서트")
            
            # 각 콘서트에 대해 정보 보강
            # (구현 생략 - 필요 시 추가)
            
            return True
            
        except Exception as e:
            logger.error(f"기존 데이터 보강 실패: {e}")
            return False
    
    def _collect_artist_data_only(self) -> bool:
        """아티스트 데이터만 수집"""
        print("🎤 아티스트 데이터만 수집 중...")
        
        try:
            # 기존 콘서트 데이터에서 아티스트 추출
            concerts_path = Config.OUTPUT_DIR / "concerts.csv"
            if not concerts_path.exists():
                print("❌ 콘서트 데이터가 없습니다.")
                return False
            
            df = pd.read_csv(concerts_path)
            unique_artists = df['artist'].dropna().unique()
            
            # 아티스트 정보 수집
            artists = []
            for artist_name in unique_artists:
                artist = self.data_collector.collect_artist_info(artist_name)
                if artist:
                    artists.append(artist)
            
            # 저장
            artists_df = pd.DataFrame([artist.__dict__ for artist in artists])
            artists_path = Config.OUTPUT_DIR / "artists.csv"
            self.writer.write_csv(artists_df, str(artists_path))
            
            print(f"✅ {len(artists)}명 아티스트 정보 수집 완료")
            return True
            
        except Exception as e:
            logger.error(f"아티스트 데이터 수집 실패: {e}")
            return False