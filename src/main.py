import sys
import os
import logging
import time
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config
from src.kopis_api import KopisAPI
from src.perplexity_api import PerplexityAPI
from src.enhanced_data_collector import EnhancedDataCollector
from src.enhanced_csv_manager import EnhancedCSVManager

logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    try:
        # 환경변수 검증
        Config.validate()
        
        print("🎵 내한 콘서트 데이터 수집기")
        print("=" * 60)
        print("📅 수집 범위:")
        print("   - 공연 중: 오늘")
        print("   - 최근 완료: 지난 30일")
        print("   - 예정: 향후 3개월")
        print("🎯 필터링: 내한공연만 (visit=Y, festival=N)")
        print("=" * 60)
        
        # API 클라이언트 초기화
        kopis_api = KopisAPI(Config.KOPIS_API_KEY)
        perplexity_api = PerplexityAPI(Config.PERPLEXITY_API_KEY)
        collector = EnhancedDataCollector(perplexity_api)
        
        print(f"\n🚀 내한 콘서트 데이터 수집을 시작합니다...")
        
        # 1. KOPIS에서 모든 상태의 콘서트 목록 가져오기
        print("1. KOPIS 공연 목록 수집 중...")
        print("   (공연 중 + 최근 완료 + 예정 콘서트)")
        concert_codes = kopis_api.fetch_all_concerts()
        
        if not concert_codes:
            print("❌ 콘서트를 찾을 수 없습니다.")
            print("💡 KOPIS API 키나 네트워크 연결을 확인해주세요.")
            return
        
        print(f"   📋 총 {len(concert_codes)}개의 공연 발견")
        
        # 2. 상세 정보 가져오기 (테스트용으로 10개만 필터링)
        print("2. KOPIS 공연 상세정보 수집 및 내한공연 필터링 중...")
        print(f"   테스트용: 10개 내한공연 발견시까지만 처리 (visit=Y, festival=N)")
        concert_details = kopis_api.fetch_concert_details(concert_codes, max_found=10)
        
        if not concert_details:
            print("❌ 내한공연 조건에 맞는 콘서트가 없습니다.")
            print("💡 수집 기간을 조정하거나 필터링 조건을 확인해주세요.")
            return
        
        print(f"   ✅ {len(concert_details)}개의 내한 콘서트 발견!")
        
        # 상태별 분류 및 표시
        ongoing = [c for c in concert_details if c['status'] == '02']
        upcoming = [c for c in concert_details if c['status'] == '01']
        completed = [c for c in concert_details if c['status'] == '03']
        
        print(f"   📊 상태별 분류:")
        print(f"      🔴 공연 중: {len(ongoing)}개")
        print(f"      🟡 공연 예정: {len(upcoming)}개")
        print(f"      🟢 공연 완료: {len(completed)}개")
        
        # 수집할 콘서트 목록 표시 (각 상태별로 최대 3개씩)
        print(f"\n📋 발견된 내한 콘서트 목록:")
        
        def show_concerts(concerts, status_name, max_show=3):
            if concerts:
                print(f"   {status_name}:")
                for i, concert in enumerate(concerts[:max_show], 1):
                    start_date = concert['start_date']
                    if len(start_date) == 8:
                        date_str = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
                    else:
                        date_str = start_date
                    print(f"      {i}. {concert['title']} - {concert['artist']} ({date_str})")
                if len(concerts) > max_show:
                    print(f"      ... 외 {len(concerts) - max_show}개")
        
        show_concerts(ongoing, "🔴 공연 중")
        show_concerts(upcoming, "🟡 공연 예정")
        show_concerts(completed, "🟢 최근 완료")
        
        # KOPIS 필터링 결과를 CSV로 저장
        print(f"\n💾 KOPIS 필터링 결과 저장 중...")
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
        
        # KOPIS 결과 CSV 저장
        import pandas as pd
        kopis_df = pd.DataFrame(kopis_csv_data)
        kopis_csv_path = os.path.join(Config.OUTPUT_DIR, 'kopis_filtered_concerts.csv')
        kopis_df.to_csv(kopis_csv_path, index=False, encoding='utf-8-sig')
        print(f"   ✅ KOPIS 필터링 결과 저장: {kopis_csv_path} ({len(kopis_csv_data)}개)")
        
        # 테스트용으로 5개만 처리
        test_limit = 5
        selected_concerts = concert_details[:test_limit]
        print(f"\n🧪 테스트용으로 {test_limit}개 콘서트만 상세 데이터 수집합니다.")
        
        # 3. Perplexity로 상세 데이터 수집
        print("3. Perplexity API로 상세 데이터 수집 중...")
        all_collected_data = []
        
        for i, concert in enumerate(selected_concerts, 1):
            status_icon = "🔴" if concert['status'] == '02' else "🟡" if concert['status'] == '01' else "🟢"
            print(f"   {status_icon} {i}/{len(selected_concerts)}: {concert['title']} - {concert['artist']}")
            
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
            print("💡 Perplexity API 키와 네트워크 연결을 확인해주세요.")
            return
        
        # 4. 각 단계별 CSV 파일로 저장
        print("4. 단계별 CSV 파일 저장 중...")
        
        # 기본 콘서트 정보만 먼저 저장
        basic_concerts = []
        for data in all_collected_data:
            concert = data['concert']
            basic_concerts.append({
                'title': concert.title,
                'artist': concert.artist,
                'start_date': concert.start_date,
                'end_date': concert.end_date,
                'venue': concert.venue,
                'status': concert.status,
                'poster': concert.poster,
                'ticket_url': concert.ticket_url
            })
        
        # 단계별 저장
        basic_df = pd.DataFrame(basic_concerts)
        basic_csv_path = os.path.join(Config.OUTPUT_DIR, 'step1_basic_concerts.csv')
        basic_df.to_csv(basic_csv_path, index=False, encoding='utf-8-sig')
        print(f"   ✅ 1단계 기본 정보 저장: {basic_csv_path} ({len(basic_concerts)}개)")
        
        # 전체 상세 데이터 저장
        EnhancedCSVManager.save_all_data(all_collected_data)
        print(f"   ✅ 2단계 전체 상세 데이터 저장 완료")
        
        print(f"\n🎉 완료! 총 {len(all_collected_data)}개 내한 콘서트의 데이터가 저장되었습니다.")
        print(f"📁 파일 위치: {Config.OUTPUT_DIR}/")
        
        print(f"\n📊 최종 수집 통계:")
        print(f"   🎯 필터링 조건: 내한 콘서트만 (visit=Y, festival=N)")
        print(f"   📋 전체 발견 내한 콘서트: {len(concert_details)}개")
        print(f"   🧪 테스트 처리 콘서트: {len(selected_concerts)}개")
        print(f"   ✅ 상세 데이터 수집 완료: {len(all_collected_data)}개")
        print(f"   🕐 상세 수집 소요 시간: 약 {len(all_collected_data) * Config.REQUEST_DELAY}초")
        
        # 파일별 행 수 확인
        print(f"\n📄 생성된 파일 확인:")
        csv_files = [
            ("kopis_filtered_concerts.csv", "KOPIS 필터링 결과"),
            ("step1_basic_concerts.csv", "1단계: 기본 콘서트 정보"),
            ("concerts.csv", "2단계: 콘서트 상세 정보"),
            ("setlists.csv", "2단계: 셋리스트 정보"),
            ("songs.csv", "2단계: 곡 정보"),
            ("cultures.csv", "2단계: 팬 문화 정보"),
            ("artists.csv", "2단계: 아티스트 정보")
        ]
        
        for filename, description in csv_files:
            filepath = os.path.join(Config.OUTPUT_DIR, filename)
            if os.path.exists(filepath):
                try:
                    import pandas as pd
                    df = pd.read_csv(filepath, encoding='utf-8-sig')
                    row_count = len(df)
                    print(f"   📋 {filename}: {description} ({row_count}개 행)")
                except:
                    print(f"   📋 {filename}: {description} (확인 불가)")
            else:
                print(f"   ❌ {filename}: 생성되지 않음")
        
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

if __name__ == "__main__":
    main()
