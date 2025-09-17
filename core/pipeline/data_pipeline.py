"""
데이터 수집 파이프라인 핵심 로직
"""
import logging
import pandas as pd
from typing import Optional

from lib.config import Config
from lib.data_collector import DataCollector
from lib.safe_writer import SafeWriter
from core.apis.kopis_api import KopisAPI

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
    
    def __init__(self):
        self.kopis_api = KopisAPI(Config.KOPIS_API_KEY)
        self.api_client = APIClient(Config.GEMINI_API_KEY if Config.USE_GEMINI_API else Config.PERPLEXITY_API_KEY)
        self.data_collector = DataCollector(self.api_client)
        self.writer = SafeWriter
    
    def run_full_pipeline(self, full_mode: bool = False) -> bool:
        """전체 파이프라인 실행"""
        print("🚀 데이터 수집 파이프라인 시작")
        
        try:
            # 1단계: KOPIS 데이터 수집
            kopis_data = self._fetch_kopis_data(full_mode)
            if not kopis_data:
                return False
            
            # 2단계: 콘서트 데이터 보강
            concerts = self._enhance_concert_data(kopis_data)
            
            # 3단계: 아티스트 데이터 수집
            artists = self._collect_artist_data(concerts)
            
            # 4단계: CSV 저장
            self._save_data(concerts, artists)
            
            print("✅ 파이프라인 완료")
            return True
            
        except Exception as e:
            logger.error(f"파이프라인 실행 실패: {e}")
            return False
    
    def run_stage(self, stage_num: int, full_mode: bool = False) -> bool:
        """특정 스테이지만 실행"""
        print(f"🎯 스테이지 {stage_num} 실행")
        
        try:
            if stage_num == 1:
                return self._fetch_kopis_data(full_mode) is not None
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
    
    def _fetch_kopis_data(self, full_mode: bool) -> Optional[list]:
        """KOPIS API에서 데이터 수집"""
        print("📡 KOPIS 데이터 수집 중...")
        
        try:
            # 전체 공연 코드 수집
            concert_codes = self.kopis_api.fetch_all_concerts()
            print(f"📊 총 {len(concert_codes)}개 공연 코드 수집")
            
            # 내한공연 필터링 (테스트 모드에서는 최대 10개만)
            max_concerts = 10 if not full_mode else None
            concerts = self.kopis_api.fetch_concert_details(
                concert_codes, 
                max_found=max_concerts
            )
            
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
            try:
                print(f"  [{i}/{len(kopis_data)}] {kopis_concert.get('title', 'Unknown')}")
                
                # 기본 콘서트 객체 생성
                concert = self.data_collector.collect_concert_basic_info(kopis_concert)
                
                # AI API로 정보 보강
                enhanced_concert = self.data_collector.enhance_concert_data(concert)
                
                enhanced_concerts.append(enhanced_concert)
                
            except Exception as e:
                logger.warning(f"콘서트 정보 보강 실패 ({kopis_concert.get('title')}): {e}")
                continue
        
        print(f"✅ {len(enhanced_concerts)}개 콘서트 정보 보강 완료")
        return enhanced_concerts
    
    def _collect_artist_data(self, concerts: list) -> list:
        """아티스트 데이터 수집"""
        print("🎤 아티스트 정보 수집 중...")
        
        # 고유 아티스트 추출
        unique_artists = set()
        for concert in concerts:
            if concert.artist:
                unique_artists.add(concert.artist)
        
        artists = []
        for i, artist_name in enumerate(unique_artists, 1):
            try:
                print(f"  [{i}/{len(unique_artists)}] {artist_name}")
                
                artist = self.data_collector.collect_artist_info(artist_name)
                if artist:
                    artists.append(artist)
                    
            except Exception as e:
                logger.warning(f"아티스트 정보 수집 실패 ({artist_name}): {e}")
                continue
        
        print(f"✅ {len(artists)}명 아티스트 정보 수집 완료")
        return artists
    
    def _save_data(self, concerts: list, artists: list):
        """데이터 저장"""
        print("💾 데이터 저장 중...")
        
        try:
            # 콘서트 데이터를 DataFrame으로 변환
            concerts_df = pd.DataFrame([concert.__dict__ for concert in concerts])
            
            # 아티스트 데이터를 DataFrame으로 변환
            artists_df = pd.DataFrame([artist.__dict__ for artist in artists])
            
            # CSV 저장
            
            self.writer.save_dataframe(concerts_df, "concerts.csv", backup_if_main=False)
            self.writer.save_dataframe(artists_df, "artists.csv", backup_if_main=False)
            
            print(f"✅ 데이터 저장 완료: {Config.OUTPUT_DIR}")
            
        except Exception as e:
            logger.error(f"데이터 저장 실패: {e}")
            raise
    
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