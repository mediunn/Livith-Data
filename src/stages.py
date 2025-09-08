"""
데이터 수집 프로세스를 단계별로 분리한 모듈
각 단계는 독립적으로 실행 가능
"""
import os
import sys
import logging
import time
import pandas as pd
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import Config
from utils.safe_writer import SafeWriter
from src.kopis_api import KopisAPI
from data_processing.enhanced_data_collector import EnhancedDataCollector
from data_processing.enhanced_csv_manager import EnhancedCSVManager
from src.artist_matcher import match_artist_names

# API 선택적 임포트
try:
    if Config.USE_GEMINI_API:
        from src.gemini_api import GeminiAPI as APIClient
    else:
        from src.perplexity_api import PerplexityAPI as APIClient
except:
    from src.perplexity_api import PerplexityAPI as APIClient

logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Stage1_FetchKopisData:
    """단계 1: KOPIS API에서 공연 데이터 수집 및 필터링"""
    
    @staticmethod
    def run(mode='incremental', test_mode=False):
        mode_text = "증분 수집 (중복 제외)" if mode == 'incremental' else "전체 갱신 (모든 데이터)"
        print("=" * 60)
        print(f"🎵 단계 1: KOPIS 데이터 수집 ({mode_text})")
        print("=" * 60)
        print("📅 수집 범위:")
        print("   - 공연 중: 오늘")
        print("   - 최근 완료: 지난 30일")
        print("   - 예정: 향후 3개월")
        print("🎯 필터링: 내한공연만 (visit=Y, festival=N)")
        print("=" * 60)
        
        # KOPIS API 초기화
        kopis_api = KopisAPI(Config.KOPIS_API_KEY)
        
        # 1. 모든 상태의 콘서트 목록 가져오기
        print("\n1-1. KOPIS 공연 목록 수집 중...")
        concert_codes = kopis_api.fetch_all_concerts()
        
        if not concert_codes:
            print("❌ 콘서트를 찾을 수 없습니다.")
            return None
        
        print(f"   📋 총 {len(concert_codes)}개의 공연 발견")
        
        # 2. 상세 정보 가져오기 및 필터링
        print("\n1-2. KOPIS 공연 상세정보 수집 및 내한공연 필터링 중...")
        
        # 테스트 모드 처리
        if test_mode:
            print("   ⚠️  테스트 모드: 내한공연 5개까지 조사 후 선택")
            codes_to_check = concert_codes  # 전체 데이터에서 조사
        else:
            print(f"   📋 전체 모드: {len(concert_codes)}개 공연 모두 조사")
            codes_to_check = concert_codes
        
        # 증분 모드: 기존 데이터 확인
        existing_codes = set()
        if mode == 'incremental':
            existing_codes = Stage1_FetchKopisData._get_existing_concert_codes()
            if existing_codes:
                print(f"   📂 기존 데이터: {len(existing_codes)}개 콘서트 코드 발견")
                print(f"   🔍 중복 제외하여 새로운 데이터만 수집")
            else:
                print("   📂 기존 데이터 없음 - 전체 수집")
        
        # 내한공연 필터링
        if test_mode:
            # 테스트 모드: 내한공연을 찾을 때까지 계속 검색
            all_concert_details = kopis_api.fetch_concert_details(codes_to_check, existing_codes if mode == 'incremental' else set(), max_found=5)
        else:
            # 전체 모드: 모든 내한공연 검색
            all_concert_details = kopis_api.fetch_concert_details(codes_to_check, existing_codes if mode == 'incremental' else set())
        
        if not all_concert_details:
            print("❌ 내한공연 조건에 맞는 콘서트가 없습니다.")
            return None
        
        print(f"   ✅ {len(all_concert_details)}개의 내한 콘서트 발견!")
        
        # 상태별 분류
        ongoing = [c for c in all_concert_details if c['status'] == '02']
        upcoming = [c for c in all_concert_details if c['status'] == '01']
        completed = [c for c in all_concert_details if c['status'] == '03']
        
        print(f"   📊 상태별 분류:")
        print(f"      🔴 공연 중: {len(ongoing)}개")
        print(f"      🟡 공연 예정: {len(upcoming)}개")
        print(f"      🟢 공연 완료: {len(completed)}개")
        
        # 테스트 모드: 내한공연 목록 표시 및 선택
        if test_mode:
            print("\n" + "=" * 60)
            print("🎵 내한공연 목록 (번호를 선택하세요)")
            print("=" * 60)
            
            # 모든 내한공연 목록 표시
            for i, concert in enumerate(all_concert_details, 1):
                status_icon = "🔴" if concert['status'] == '02' else "🟡" if concert['status'] == '01' else "🟢"
                start_date = concert['start_date']
                if len(start_date) == 8:
                    date_str = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
                else:
                    date_str = start_date
                print(f"{i:2d}. {status_icon} {concert['title'][:40]:<40} | {concert['artist'][:30]:<30} | {date_str}")
            
            print("=" * 60)
            
            # 사용자 선택
            while True:
                try:
                    choice = input("\n🎯 처리할 콘서트 번호를 입력하세요 (1-{}): ".format(len(all_concert_details)))
                    choice_idx = int(choice) - 1
                    if 0 <= choice_idx < len(all_concert_details):
                        selected_concert = all_concert_details[choice_idx]
                        print(f"\n✅ 선택된 콘서트: {selected_concert['title']} - {selected_concert['artist']}")
                        concert_details = [selected_concert]
                        break
                    else:
                        print(f"❌ 1부터 {len(all_concert_details)} 사이의 번호를 입력하세요.")
                except ValueError:
                    print("❌ 올바른 숫자를 입력하세요.")
                except KeyboardInterrupt:
                    print("\n❌ 취소되었습니다.")
                    return None
        else:
            # 전체 모드: 데이터 검토 및 필터링
            concert_details = Stage1_FetchKopisData._review_and_filter_concerts(all_concert_details)
            
            if not concert_details:
                print("❌ 처리할 콘서트가 없습니다.")
                return None
            
            # 콘서트 목록 표시
            Stage1_FetchKopisData._show_concerts([c for c in concert_details if c['status'] == '02'], "🔴 공연 중")
            Stage1_FetchKopisData._show_concerts([c for c in concert_details if c['status'] == '01'], "🟡 공연 예정")
            Stage1_FetchKopisData._show_concerts([c for c in concert_details if c['status'] == '03'], "🟢 최근 완료")
        
        # CSV 저장
        print(f"\n1-3. KOPIS 필터링 결과 저장 중...")
        kopis_csv_data = []
        for concert in concert_details:
            kopis_csv_data.append({
                'code': concert['code'],
                'title': concert['title'],
                'artist': concert['artist'],
                'start_date': concert['start_date'],
                'end_date': concert['end_date'],
                'venue': concert['venue'],
                'status': concert['status'],
                'visit': concert['visit'],
                'festival': concert['festival']
            })
        
        kopis_df = pd.DataFrame(kopis_csv_data)
        saved_path = SafeWriter.save_dataframe(kopis_df, 'kopis_filtered_concerts.csv')
        print(f"   ✅ 저장 완료: {saved_path} ({len(kopis_csv_data)}개)")
        
        return concert_details
    
    @staticmethod
    def _get_existing_concert_codes():
        """기존 CSV 파일에서 콘서트 코드 목록을 가져옴"""
        import pandas as pd
        import os
        
        existing_codes = set()
        csv_files = [
            'kopis_filtered_concerts.csv',
            'concerts.csv',
            'step1_basic_concerts.csv'
        ]
        
        for filename in csv_files:
            filepath = os.path.join(Config.OUTPUT_DIR, filename)
            if os.path.exists(filepath):
                try:
                    df = pd.read_csv(filepath, encoding='utf-8-sig')
                    if 'code' in df.columns:
                        codes = df['code'].dropna().astype(str).tolist()
                        existing_codes.update(codes)
                        logger.info(f"{filename}에서 {len(codes)}개 코드 로드")
                except Exception as e:
                    logger.warning(f"{filename} 로드 실패: {e}")
        
        return existing_codes
    
    @staticmethod
    def _review_and_filter_concerts(all_concert_details):
        """내한공연 목록을 5개씩 페이지네이션하여 검토하고 제외할 항목 선택"""
        print("\n" + "=" * 60)
        print("📋 내한공연 데이터 검토")
        print("=" * 60)
        print(f"총 {len(all_concert_details)}개의 내한공연이 발견되었습니다.")
        print("5개씩 표시됩니다. 제외할 콘서트가 있으면 번호를 입력하세요.")
        print("=" * 60)
        
        excluded_indices = set()
        page_size = 5
        total_pages = (len(all_concert_details) + page_size - 1) // page_size
        
        for page in range(total_pages):
            start_idx = page * page_size
            end_idx = min(start_idx + page_size, len(all_concert_details))
            
            print(f"\n📄 페이지 {page + 1}/{total_pages} (전체 {len(all_concert_details)}개 중 {start_idx + 1}-{end_idx}번)")
            print("-" * 60)
            
            # 현재 페이지의 콘서트 표시
            for i in range(start_idx, end_idx):
                concert = all_concert_details[i]
                status_icon = "🔴" if concert['status'] == '02' else "🟡" if concert['status'] == '01' else "🟢"
                
                # 날짜 포맷팅
                start_date = concert['start_date']
                if len(start_date) == 8:
                    date_str = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
                else:
                    date_str = start_date
                
                # 제외된 항목 표시
                excluded_mark = " ❌ [제외됨]" if i in excluded_indices else ""
                
                print(f"{i + 1:3d}. {status_icon} {concert['title'][:35]:<35} | {concert['artist'][:25]:<25} | {date_str}{excluded_mark}")
            
            print("-" * 60)
            
            # 사용자 입력 처리
            while True:
                try:
                    user_input = input("\n제외할 번호를 입력하세요 (쉼표로 구분, Enter: 다음 페이지, 'b': 이전 페이지, 'f': 완료): ").strip()
                    
                    if user_input.lower() == 'f':
                        # 검토 완료
                        filtered_concerts = [c for i, c in enumerate(all_concert_details) if i not in excluded_indices]
                        print(f"\n✅ 검토 완료: {len(all_concert_details)}개 중 {len(excluded_indices)}개 제외, {len(filtered_concerts)}개 처리 예정")
                        return filtered_concerts
                    
                    elif user_input.lower() == 'b':
                        # 이전 페이지
                        if page > 0:
                            page -= 2  # for 루프에서 +1 되므로 -2
                            break
                        else:
                            print("⚠️  첫 페이지입니다.")
                    
                    elif user_input == '':
                        # 다음 페이지
                        break
                    
                    else:
                        # 제외할 번호 처리
                        numbers = [n.strip() for n in user_input.split(',') if n.strip()]
                        for num_str in numbers:
                            try:
                                num = int(num_str) - 1  # 0-based index로 변환
                                if 0 <= num < len(all_concert_details):
                                    if num in excluded_indices:
                                        excluded_indices.remove(num)
                                        print(f"↩️  {num + 1}번 복원됨")
                                    else:
                                        excluded_indices.add(num)
                                        print(f"❌ {num + 1}번 제외됨")
                                else:
                                    print(f"⚠️  {num_str}번은 유효하지 않은 번호입니다.")
                            except ValueError:
                                print(f"⚠️  '{num_str}'는 올바른 숫자가 아닙니다.")
                        break
                        
                except KeyboardInterrupt:
                    print("\n❌ 검토가 취소되었습니다.")
                    return None
                except Exception as e:
                    print(f"❌ 오류 발생: {e}")
                    continue
        
        # 모든 페이지 검토 완료
        filtered_concerts = [c for i, c in enumerate(all_concert_details) if i not in excluded_indices]
        print(f"\n✅ 검토 완료: {len(all_concert_details)}개 중 {len(excluded_indices)}개 제외, {len(filtered_concerts)}개 처리 예정")
        return filtered_concerts
    
    @staticmethod
    def _show_concerts(concerts, status_name, max_show=3):
        if concerts:
            print(f"\n   {status_name}:")
            for i, concert in enumerate(concerts[:max_show], 1):
                start_date = concert['start_date']
                if len(start_date) == 8:
                    date_str = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
                else:
                    date_str = start_date
                print(f"      {i}. {concert['title']} - {concert['artist']} ({date_str})")
            if len(concerts) > max_show:
                print(f"      ... 외 {len(concerts) - max_show}개")


class Stage2_CollectBasicInfo:
    """단계 2: 기본 콘서트 정보 수집"""
    
    @staticmethod
    def run(concert_details=None, mode='incremental', test_mode=False):
        mode_text = "증분 수집" if mode == 'incremental' else "전체 갱신"
        print("=" * 60)
        print(f"🎵 단계 2: 기본 콘서트 정보 수집 ({mode_text})")
        print("=" * 60)
        
        # 이전 단계 결과가 없으면 CSV에서 로드
        if concert_details is None:
            kopis_csv_path = os.path.join(Config.OUTPUT_DIR, 'kopis_filtered_concerts.csv')
            if not os.path.exists(kopis_csv_path):
                print("❌ KOPIS 데이터 파일이 없습니다. 단계 1을 먼저 실행하세요.")
                return None
            
            print(f"📂 이전 결과 로드: {kopis_csv_path}")
            df = pd.read_csv(kopis_csv_path, encoding='utf-8-sig')
            concert_details = df.to_dict('records')
        
        # API 클라이언트 초기화 (Gemini 또는 Perplexity)
        api_key = Config.GEMINI_API_KEY if Config.USE_GEMINI_API else Config.PERPLEXITY_API_KEY
        api_client = APIClient(api_key)
        collector = EnhancedDataCollector(api_client)
        
        print(f"\n2-1. Perplexity API로 기본 데이터 수집 중...")
        print(f"   총 {len(concert_details)}개 콘서트")
        
        # 테스트 모드일 때만 1개로 제한
        if test_mode:
            print("   ⚠️  테스트 모드: 1개 콘서트만 처리")
            concert_details = concert_details[:1]
        
        all_collected_data = []
        for i, concert in enumerate(concert_details, 1):
            status_icon = "🔴" if concert['status'] == '02' else "🟡" if concert['status'] == '01' else "🟢"
            print(f"   {status_icon} {i}/{len(concert_details)}: {concert['title']} - {concert['artist']}")
            
            try:
                collected_data = collector.collect_concert_data(concert)
                all_collected_data.append(collected_data)
                print(f"      ✅ 완료")
                time.sleep(Config.REQUEST_DELAY)
            except Exception as e:
                logger.error(f"데이터 수집 실패: {e}")
                print(f"      ❌ 실패: {str(e)}")
                continue
        
        if not all_collected_data:
            print("❌ 수집된 데이터가 없습니다.")
            return None
        
        # 기본 콘서트 정보 저장
        print("\n2-2. 기본 콘서트 정보 저장 중...")
        basic_concerts = []
        for data in all_collected_data:
            concert = data['concert']
            basic_concerts.append({
                'artist': concert.artist,
                'code': concert.code,
                'title': concert.title,
                'start_date': concert.start_date,
                'end_date': concert.end_date,
                'status': concert.status,
                'poster': concert.poster,
                'ticket_site': concert.ticket_site,
                'ticket_url': concert.ticket_url,
                'venue': concert.venue,
                'label': concert.label,
                'introduction': concert.introduction
            })
        
        basic_df = pd.DataFrame(basic_concerts)
        saved_path = SafeWriter.save_dataframe(basic_df, 'step1_basic_concerts.csv')
        print(f"   ✅ 저장 완료: {saved_path} ({len(basic_concerts)}개)")
        
        # 2-3. 수집된 모든 데이터 저장 (셋리스트, 곡, 문화, 아티스트 등)
        print("\n2-3. 수집된 상세 데이터 저장 중...")
        from data_processing.enhanced_csv_manager import EnhancedCSVManager
        EnhancedCSVManager.save_all_data(all_collected_data)
        print("   ✅ 저장 완료: concerts.csv, setlists.csv, songs.csv, cultures.csv, artists.csv")
        
        return all_collected_data


class Stage3_CollectDetailedInfo:
    """단계 3: 상세 데이터 수집 (아티스트, 셋리스트, 곡, 문화 등)"""
    
    @staticmethod
    def run(all_collected_data=None, mode='incremental', test_mode=False):
        mode_text = "증분 수집" if mode == 'incremental' else "전체 갱신"
        print("=" * 60)
        print(f"🎵 단계 3: 상세 데이터 수집 ({mode_text})")
        print("=" * 60)
        
        # 이전 단계 결과가 없으면 기본 정보에서 재수집 필요
        if all_collected_data is None:
            print("⚠️  수집된 데이터가 없습니다. 단계 2 결과를 사용합니다.")
            basic_csv_path = os.path.join(Config.OUTPUT_DIR, 'step1_basic_concerts.csv')
            if not os.path.exists(basic_csv_path):
                print("❌ 기본 콘서트 데이터가 없습니다. 단계 2를 먼저 실행하세요.")
                return None
            
            # 재수집이 필요한 경우
            print("📂 기본 정보에서 상세 데이터 재수집 필요")
            df = pd.read_csv(basic_csv_path, encoding='utf-8-sig')
            concert_details = df.to_dict('records')
            
            # 테스트 모드일 때만 1개로 제한
            if test_mode:
                print("   ⚠️  테스트 모드: 1개 콘서트만 처리")
                concert_details = concert_details[:1]
            
            # API로 재수집 (Gemini 또는 Perplexity)
            api_key = Config.GEMINI_API_KEY if Config.USE_GEMINI_API else Config.PERPLEXITY_API_KEY
            api_client = APIClient(api_key)
            collector = EnhancedDataCollector(api_client)
            
            all_collected_data = []
            for i, concert in enumerate(concert_details, 1):
                print(f"   재수집 {i}/{len(concert_details)}: {concert['title']}")
                try:
                    collected_data = collector.collect_concert_data(concert)
                    all_collected_data.append(collected_data)
                    time.sleep(Config.REQUEST_DELAY)
                except Exception as e:
                    logger.error(f"재수집 실패: {e}")
                    continue
        
        print("\n3-1. 상세 데이터 CSV 저장 중...")
        EnhancedCSVManager.save_all_data(all_collected_data)
        print("   ✅ 저장 완료: concerts.csv, setlists.csv, songs.csv, cultures.csv, artists.csv")
        
        return all_collected_data


class Stage4_CollectMerchandise:
    """단계 4: 굿즈(MD) 정보 수집"""
    
    @staticmethod
    def run(all_collected_data=None, mode='incremental', test_mode=False):
        mode_text = "증분 수집" if mode == 'incremental' else "전체 갱신"
        print("=" * 60)
        print(f"🎵 단계 4: 굿즈(MD) 정보 수집 ({mode_text})")
        print("=" * 60)
        
        # 이전 단계 결과가 없으면 콘서트 정보에서 로드
        if all_collected_data is None:
            concerts_csv_path = os.path.join(Config.OUTPUT_DIR, 'concerts.csv')
            if not os.path.exists(concerts_csv_path):
                print("❌ 콘서트 데이터가 없습니다. 단계 3을 먼저 실행하세요.")
                return None
            
            print("📂 콘서트 정보 로드 중...")
            df = pd.read_csv(concerts_csv_path, encoding='utf-8-sig')
            concerts = df.to_dict('records')
            
            # 테스트 모드일 때만 1개로 제한
            if test_mode:
                print("   ⚠️  테스트 모드: 1개 콘서트만 처리")
                concerts = concerts[:1]
            
            # 최소 정보로 데이터 구조 생성
            all_collected_data = []
            for concert in concerts:
                from data_processing.data_models import Concert
                concert_obj = Concert(
                    artist=concert.get('artist', ''),
                    code=concert.get('code', ''),
                    title=concert.get('title', ''),
                    start_date=concert.get('start_date', ''),
                    end_date=concert.get('end_date', ''),
                    venue=concert.get('venue', ''),
                    status=concert.get('status', ''),
                    poster=concert.get('poster', ''),
                    ticket_site=concert.get('ticket_site', ''),
                    ticket_url=concert.get('ticket_url', ''),
                    label=concert.get('label', ''),
                    introduction=concert.get('introduction', '')
                )
                all_collected_data.append({'concert': concert_obj})
        
        # API 클라이언트 초기화 (Gemini 또는 Perplexity)
        api_key = Config.GEMINI_API_KEY if Config.USE_GEMINI_API else Config.PERPLEXITY_API_KEY
        api_client = APIClient(api_key)
        collector = EnhancedDataCollector(api_client)
        
        print(f"\n4-1. 굿즈 정보 수집 중...")
        merchandise_data = []
        
        for i, data in enumerate(all_collected_data, 1):
            concert = data['concert']
            print(f"   🛍️ {i}/{len(all_collected_data)}: {concert.title}")
            
            try:
                merchandise_info = collector.collect_merchandise_data(concert)
                if merchandise_info:
                    merchandise_data.extend(merchandise_info)
                    print(f"      ✅ 굿즈 {len(merchandise_info)}개 발견")
                else:
                    print(f"      ⚪ 굿즈 정보 없음")
                time.sleep(Config.REQUEST_DELAY)
            except Exception as e:
                logger.error(f"굿즈 정보 수집 실패: {e}")
                print(f"      ❌ 실패: {str(e)}")
                continue
        
        # 굿즈 데이터 저장
        if merchandise_data:
            print("\n4-2. 굿즈 정보 저장 중...")
            merchandise_df = pd.DataFrame(merchandise_data)
            saved_path = SafeWriter.save_dataframe(merchandise_df, 'md.csv')
            print(f"   ✅ 저장 완료: {saved_path} ({len(merchandise_data)}개)")
        else:
            print("   ⚪ 수집된 굿즈 정보가 없습니다.")
        
        return merchandise_data


class Stage5_MatchArtistNames:
    """단계 5: 아티스트명 매칭 및 정리"""
    
    @staticmethod
    def run(test_mode=False):
        print("=" * 60)
        print("🎵 단계 5: 아티스트명 매칭")
        print("=" * 60)
        
        print("\n5-1. artist.csv 기준으로 concerts.csv 아티스트명 매칭 중...")
        try:
            match_artist_names()
            print("   ✅ 아티스트명 매칭 완료")
            
            # 결과 확인
            concerts_csv = os.path.join(Config.OUTPUT_DIR, 'concerts.csv')
            if os.path.exists(concerts_csv):
                df = pd.read_csv(concerts_csv, encoding='utf-8-sig')
                if test_mode:
                    print(f"   📊 테스트 모드: 1개 콘서트 처리 완료")
                else:
                    print(f"   📊 업데이트된 콘서트: {len(df)}개")
            
            return True
        except Exception as e:
            logger.error(f"아티스트명 매칭 실패: {e}")
            print(f"   ❌ 실패: {str(e)}")
            return False


class StageRunner:
    """모든 단계를 관리하고 실행하는 클래스"""
    
    @staticmethod
    def run_all(mode='incremental', test_mode=None, force_reset=False):
        """모든 단계를 순차적으로 실행"""
        try:
            # 테스트 모드 선택 (입력받지 않은 경우)
            if test_mode is None:
                print("\n" + "=" * 60)
                print("🎵 내한 콘서트 데이터 수집기")
                print("=" * 60)
                print("\n실행 모드를 선택하세요:")
                print("1. 테스트 모드 (내한공연 1개만 처리)")
                print("2. 전체 모드 (모든 내한공연 처리)")
                
                while True:
                    try:
                        choice = input("\n선택 (1 또는 2): ")
                        if choice == '1':
                            test_mode = True
                            print("\n⚠️  테스트 모드로 실행합니다.")
                            break
                        elif choice == '2':
                            test_mode = False
                            print("\n📋 전체 모드로 실행합니다.")
                            break
                        else:
                            print("❌ 1 또는 2를 입력하세요.")
                    except KeyboardInterrupt:
                        print("\n❌ 취소되었습니다.")
                        return False
            
            # 테스트 모드에 따른 출력 디렉토리 설정 (먼저 설정)
            Config.set_test_mode(test_mode)
            
            # 테스트 모드인 경우 데이터 재사용 여부 확인
            reset_data = force_reset
            skip_stage1 = False
            if test_mode and not force_reset:
                data_option = StageRunner._ask_test_data_option()
                if data_option == "reset":
                    reset_data = True
                elif data_option == "reuse":
                    skip_stage1 = True  # 기존 데이터 재사용시 단계 1 건너뛰기
            
            mode_text = "증분 수집" if mode == 'incremental' else "전체 갱신"
            test_text = " (테스트)" if test_mode else " (전체)"
            output_text = f"출력: {Config.OUTPUT_DIR}"
            print(f"\n🎵 내한 콘서트 데이터 수집기 - {mode_text}{test_text}")
            print(f"📁 {output_text}")
            print("=" * 60)
            
            # 환경변수 검증 (단계 1을 건너뛰지 않는 경우만)
            if not skip_stage1:
                Config.validate()
            
            # 데이터 리셋 (필요한 경우)
            if reset_data:
                StageRunner._reset_test_data()
            
            # 단계 1: KOPIS 데이터 수집 (건너뛰기 가능)
            if skip_stage1:
                print("📂 기존 KOPIS 데이터를 재사용합니다. 단계 1을 건너뜁니다.\n")
                # 기존 KOPIS 데이터 로드
                concert_details = StageRunner._load_existing_kopis_data()
                if not concert_details:
                    print("❌ 기존 KOPIS 데이터를 로드할 수 없습니다. 단계 1부터 다시 실행합니다.")
                    concert_details = Stage1_FetchKopisData.run(mode, test_mode)
            else:
                concert_details = Stage1_FetchKopisData.run(mode, test_mode)
                
            if not concert_details:
                return False
            
            # 단계 2: 기본 정보 수집
            all_collected_data = Stage2_CollectBasicInfo.run(concert_details, mode, test_mode)
            if not all_collected_data:
                return False
            
            # 단계 3: 상세 정보 수집
            Stage3_CollectDetailedInfo.run(all_collected_data, mode, test_mode)
            
            # 단계 4: 굿즈 정보 수집
            Stage4_CollectMerchandise.run(all_collected_data, mode, test_mode)
            
            # 단계 5: 아티스트명 매칭
            Stage5_MatchArtistNames.run(test_mode)
            
            # 최종 통계 표시
            StageRunner._show_final_statistics()
            
            return True
            
        except Exception as e:
            logger.error(f"전체 실행 중 오류: {e}")
            print(f"❌ 오류 발생: {e}")
            return False
    
    @staticmethod
    def _show_final_statistics():
        """최종 통계 표시"""
        print("\n" + "=" * 60)
        print("📊 최종 수집 통계")
        print("=" * 60)
        
        csv_files = [
            ("kopis_filtered_concerts.csv", "KOPIS 필터링 결과"),
            ("step1_basic_concerts.csv", "기본 콘서트 정보"),
            ("concerts.csv", "콘서트 상세 정보"),
            ("setlists.csv", "셋리스트 정보"),
            ("songs.csv", "곡 정보"),
            ("cultures.csv", "팬 문화 정보"),
            ("artists.csv", "아티스트 정보"),
            ("md.csv", "굿즈 정보")
        ]
        
        for filename, description in csv_files:
            filepath = os.path.join(Config.OUTPUT_DIR, filename)
            if os.path.exists(filepath):
                try:
                    df = pd.read_csv(filepath, encoding='utf-8-sig')
                    print(f"   📋 {description}: {len(df)}개")
                except:
                    print(f"   📋 {description}: 확인 불가")
            else:
                print(f"   ❌ {description}: 생성되지 않음")
        
        print("\n🎉 데이터 수집 완료!")
        print(f"📁 저장 위치: {Config.OUTPUT_DIR}/")
    
    @staticmethod
    def _ask_test_data_option():
        """테스트 모드에서 데이터 재사용 또는 리셋 여부를 묻는 함수"""
        import os
        
        # 기존 데이터 파일 확인
        existing_files = []
        test_files = [
            'concerts.csv',
            'kopis_filtered_concerts.csv', 
            'step1_basic_concerts.csv'
        ]
        
        for filename in test_files:
            filepath = os.path.join(Config.OUTPUT_DIR, filename)
            if os.path.exists(filepath):
                existing_files.append(filename)
        
        if not existing_files:
            print("📂 기존 테스트 데이터가 없습니다. KOPIS부터 새로 수집합니다.")
            return False
        
        print("\n" + "-" * 60)
        print("📂 기존 테스트 데이터가 발견되었습니다:")
        for filename in existing_files:
            filepath = os.path.join(Config.OUTPUT_DIR, filename)
            try:
                import pandas as pd
                df = pd.read_csv(filepath, encoding='utf-8-sig')
                print(f"   - {filename}: {len(df)}개 콘서트")
            except:
                print(f"   - {filename}: 확인 불가")
        
        print("\n테스트 데이터 처리 방법을 선택하세요:")
        print("1. 기존 데이터 재사용 (다음 단계부터 실행)")
        print("2. 데이터 초기화 후 KOPIS부터 새로 수집")
        
        while True:
            try:
                choice = input("\n선택 (1 또는 2): ")
                if choice == '1':
                    print("✅ 기존 데이터를 재사용합니다.")
                    return "reuse"  # 기존 데이터 재사용
                elif choice == '2':
                    print("🗑️  데이터를 초기화하고 새로 수집합니다.")
                    return "reset"  # 데이터 리셋
                else:
                    print("❌ 1 또는 2를 입력하세요.")
            except KeyboardInterrupt:
                print("\n❌ 취소되었습니다.")
                return "reuse"  # 기본값: 재사용
    
    @staticmethod
    def _load_existing_kopis_data():
        """기존 KOPIS 데이터를 로드"""
        try:
            import pandas as pd
            kopis_file = os.path.join(Config.OUTPUT_DIR, 'kopis_filtered_concerts.csv')
            
            if not os.path.exists(kopis_file):
                return None
            
            df = pd.read_csv(kopis_file, encoding='utf-8-sig')
            if len(df) == 0:
                return None
            
            # DataFrame을 concert_details 형식으로 변환
            concert_details = []
            for _, row in df.iterrows():
                concert_details.append({
                    'code': row['code'],
                    'title': row['title'],
                    'artist': row['artist'],
                    'start_date': row['start_date'],
                    'end_date': row['end_date'],
                    'venue': row['venue'],
                    'status': row['status'],
                    'visit': row['visit'],
                    'festival': row['festival']
                })
            
            print(f"   📂 기존 KOPIS 데이터 로드됨: {len(concert_details)}개 콘서트")
            return concert_details
            
        except Exception as e:
            logger.error(f"기존 KOPIS 데이터 로드 실패: {e}")
            return None
    
    @staticmethod
    def _reset_test_data():
        """테스트 데이터 파일들을 삭제하는 함수"""
        import os
        
        files_to_reset = [
            'concerts.csv',
            'kopis_filtered_concerts.csv', 
            'step1_basic_concerts.csv',
            'setlists.csv',
            'songs.csv', 
            'cultures.csv',
            'artists.csv',
            'md.csv'
        ]
        
        deleted_files = []
        for filename in files_to_reset:
            filepath = os.path.join(Config.OUTPUT_DIR, filename)
            if os.path.exists(filepath):
                try:
                    os.remove(filepath)
                    deleted_files.append(filename)
                except Exception as e:
                    logger.error(f"파일 삭제 실패: {filename} - {e}")
        
        if deleted_files:
            print(f"🗑️  삭제된 파일: {', '.join(deleted_files)}")
        else:
            print("📂 삭제할 파일이 없습니다.")