#!/usr/bin/env python3
"""
concert_info.csv에서 잘린 텍스트를 수동으로 자연스럽게 수정하는 스크립트
AI 없이 직접 100자에 맞춰 완성
"""
import pandas as pd
from pathlib import Path
import time

def fix_truncated_text(artist_name, concert_title, category, truncated_content):
    """잘린 텍스트를 자연스럽게 완성 (100자 제한)"""
    
    # 각 케이스별로 직접 수정
    fixes = {
        "ASCA (아스카)": "무신사 개러지 공연장 내에는 유료 물품보관함이 비치되어 있어 개인 소지품을 보관할 수 있었어요. 굿즈 판매 여부는 공연마다 다르니 확인해보세요.",
        "CHRISTOPHER (크리스토퍼)": "공연장 주변에는 여러 편의점을 쉽게 찾을 수 있지만 공연장과 가장 가까운 편의점은 CU 올림픽공원1호점이예요. 핸드볼경기장과 KSPO DOME 사이에 있어요.",
        "Elijah Woods (엘리야 우즈)": "무신사 개러지 공연장 내에는 유료 물품보관함이 비치되어 있어 개인 소지품을 보관할 수 있어요. 500원 동전 2개를 투입해야 이용할 수 있으니 준비하세요.",
        "Eric Martin (에릭 마틴)": "무신사 개러지 공연장 내에는 유료 물품보관함이 비치되어 있어 개인 소지품을 보관할 수 있어요. 500원 동전 2개를 투입해야 이용할 수 있으니 준비하세요.",
        "Jacky Cheung (장학우)": "공연장 내에서는 관객 여러분들을 위해 굿즈 판매 부스가 운영될 예정이었어요. 인스파이어 리조트 내 다양한 식음료 시설도 이용 가능해요.",
        "JAKE MILLER (제이크 밀러)": "무신사 개러지 공연장 내에는 유료 물품보관함이 비치되어 있어 개인 소지품을 보관할 수 있어요. 500원 동전 2개를 투입해야 이용할 수 있으니 준비하세요.",
        "John Carroll Kirby (존 캐럴 커비)": "제주 서귀포시에 위치한 공연장 하우스오브레퓨즈는 제주시 애월읍 숲속에 20여 년간 방치됐던 폐건물에 새 생명을 불어넣어 탄생한 멋진 공간이예요.",
        "JVKE (제이크)": "공연장인 올림픽공원 주차요금은 10분당 600원이고 일 최대 20,000원이예요. 주차 공간은 충분하나 혼잡할 수 있으니 대중교통 이용을 권장해요.",
        "Mei Semones (메이 시몬스)": "Mei Semones Live [서울] 콘서트의 티켓 가격은 전석 스탠딩 77,000원이었어요. 예스24 티켓을 통해 2024년 3월 12일에 오픈되었어요.",
        "MUSE (뮤즈)": "인천 SSG 랜더스필드는 지하 및 지상 주차장을 자체적으로 운영하고 있어요. 총 4002대 주차 가능하고 하루에 2,000원으로 저렴하게 이용할 수 있어요.",
        "NEMOPHILA (네모필라)": "예스24 원더로크홀에는 물품보관소와 동전교환기가 있으며 공연장 내 식음료 판매는 공연별로 다를 수 있어요. 주차장이 없으니 대중교통 이용을 권장해요.",
        "Pink Sweat$ (핑크스웨츠)": "공연이 진행되는 YES24 LIVE HALL은 전용 주차 공간이 없어서 대중교통 이용을 권장해요. 지하철 5호선 광나루역 2번 출구에서 도보 3분이예요.",
        "SCANDAL (스캔들)": "공연장 주변 주차 공간이 협소하므로 가급적 대중교통 이용을 권장하며, 차량 이용 시에는 인근 유료 공영주차장을 이용하셔야 해요. 물품보관함도 있어요.",
        "SEKAI NO OWARI (세카이 노 오와리)": "고려대학교 화정체육관은 매우 높은 지대에 위치하고 있어 도보 이동은 추천하지 않아요. 성북20 버스나 성신여대역에서 택시를 이용하시는 것이 편해요.",
        "toconoma (토코노마)": "공연장인 홍대 상상마당의 건물 주차장은 이용할 수 없지만 30분에 2,000원, 1시간에 4,000원으로 근처 LG팰리스 공영주차장을 이용할 수 있어요.",
        "toe (토)": "무신사 개러지 공연장 내에는 유료 물품보관함이 비치되어 있어 개인 소지품을 보관할 수 있어요. 500원 동전 2개를 투입해야 이용할 수 있으니 준비하세요.",
        "Travis Scott (트래비스 스캇)": "고양종합운동장은 자체 주차장을 24시간 운영해요. 주차 가능 대수는 약 1,000대로, 최초 1시간 1,000원, 이후 5분당 170원이 부과돼요.",
        "Tyler, The Creator (타일러, 더 크리에이터)": "고양종합운동장은 자체 주차장을 24시간 운영해요. 주차 가능 대수는 약 1,000대로, 최초 1시간 1,000원, 이후 5분당 170원이 부과돼요.",
        "yama (야마)": "예스24 원더로크홀에는 물품보관소와 동전교환기가 있으며 공연장 내 식음료 판매는 공연별로 다를 수 있어요. 주차장이 없으니 대중교통 이용을 권장해요."
    }
    
    # 아티스트별 수정 내용이 있으면 반환
    if artist_name in fixes:
        fixed_text = fixes[artist_name]
        # 100자로 정확히 맞추기
        if len(fixed_text) > 100:
            fixed_text = fixed_text[:97] + "요."
        elif len(fixed_text) < 100:
            # 부족하면 패딩 추가
            padding_needed = 100 - len(fixed_text)
            if padding_needed <= 3:
                fixed_text = fixed_text[:-1] + "어요."
            else:
                fixed_text = fixed_text
        return fixed_text[:100]
    
    # 매칭되지 않은 경우 기본 처리
    if len(truncated_content) == 100:
        # 이미 100자인 경우 끝만 다듬기
        if not truncated_content.endswith(('요', '다', '해요', '니다', '어요', '습니다')):
            return truncated_content[:97] + "요."
    
    return truncated_content

def main():
    """메인 실행 함수"""
    csv_path = Path('/Users/youz2me/Xcode/Livith-Data/output/main_output/concert_info.csv')
    
    if not csv_path.exists():
        print(f"❌ 파일을 찾을 수 없습니다: {csv_path}")
        return
    
    # CSV 읽기
    print("📁 CSV 파일 로드 중...")
    df = pd.read_csv(csv_path, encoding='utf-8')
    print(f"   총 {len(df)}개 레코드 로드됨")
    
    # 백업 생성
    backup_path = csv_path.parent / f"concert_info_backup_{int(time.time())}.csv"
    df.to_csv(backup_path, index=False, encoding='utf-8')
    print(f"💾 백업 생성: {backup_path}")
    print()
    
    # 잘린 텍스트 수정
    print("🔧 텍스트 수정 중...")
    print("=" * 70)
    
    modified_count = 0
    
    for idx, row in df.iterrows():
        content = str(row['content']).strip()
        
        # 100자이고 부자연스러운 끝맺음인 경우
        if len(content) == 100:
            natural_endings = ['.', '요', '다', '해요', '니다', '어요', '습니다', '네요', '됩니다', '!', '?', '세요']
            if not any(content.endswith(ending) for ending in natural_endings):
                print(f"수정 중: {row['artist_name']} - {row['category']}")
                
                # 텍스트 수정
                fixed_content = fix_truncated_text(
                    row['artist_name'],
                    row['concert_title'],
                    row['category'],
                    content
                )
                
                if fixed_content != content:
                    df.at[idx, 'content'] = fixed_content
                    print(f"   ✅ 수정 완료 ({len(fixed_content)}자)")
                    modified_count += 1
                else:
                    print(f"   ⏭️  변경 없음")
    
    print()
    print("=" * 70)
    print(f"📊 처리 완료: {modified_count}개 수정됨")
    
    if modified_count > 0:
        # 원본 파일 업데이트
        df.to_csv(csv_path, index=False, encoding='utf-8')
        print(f"✅ 파일 업데이트 완료: {csv_path}")
        
        # 최종 통계
        print("\n📈 업데이트 후 통계:")
        content_lengths = df['content'].str.len()
        print(f"   평균 글자 수: {content_lengths.mean():.1f}자")
        print(f"   최대 글자 수: {content_lengths.max()}자")
        print(f"   100자 정확히: {(content_lengths == 100).sum()}개")
        print(f"   100자 초과: {(content_lengths > 100).sum()}개 (없어야 정상)")
        
        # 100자 초과 확인
        over_100 = df[df['content'].str.len() > 100]
        if len(over_100) > 0:
            print("\n⚠️  경고: 100자 초과 항목 발견!")
            for idx, row in over_100.iterrows():
                print(f"   - {row['artist_name']}: {len(row['content'])}자")
    else:
        print("❌ 수정된 데이터가 없습니다.")

if __name__ == "__main__":
    main()