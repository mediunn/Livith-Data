#!/usr/bin/env python3
import pandas as pd

def fix_concerts_csv():
    try:
        print("step1_basic_concerts.csv 파일 읽기...")
        # step1_basic_concerts.csv는 원본 형태의 아티스트 이름을 가지고 있음
        df = pd.read_csv('/Users/youz2me/Xcode/Livith-Data/output/step1_basic_concerts.csv', encoding='utf-8')
        
        print("artists.csv에서 아티스트 매핑 정보 읽기...")
        artists_df = pd.read_csv('/Users/youz2me/Xcode/Livith-Data/output/artists.csv', encoding='utf-8', header=None)
        # artists.csv의 첫 번째 컬럼이 표준화된 아티스트 이름
        artists_mapping = {}
        
        # 매핑 테이블 생성 (간단한 방법)
        artist_names = {
            "장학우": "Jacky Cheung (장학우)",
            "프레드 어게인": "Fred again.. (프레드 어게인)",
            "羊文学 (히츠지분가쿠)": "히츠지분가쿠 (羊文学)",
            "스즈키 코노미": "Suzuki Konomi (스즈키 코노미)",
            "2hollis": "2hollis (투홀리스)",
            "예지": "YAEJI (예지)",
            "호시노 겐": "호시노 겐 (星野源)",
            "알렉산드로스": "Alexandros (알렉산드로스)",
            "CHANMINA": "CHANMINA (챤미나)",
            "히토리에": "HITORIE (ヒトリエ)",
            "사키야마 소우시": "崎山蒼志 (사키야마 소우시)",
            "타카세 토야": "Takase Toya (타카세 토야)",
            "토미 이매뉴얼": "Tommy Emmanuel (토미 이매뉴얼)",
            "타일러 더 크리에이터": "Tyler, The Creator (타일러, 더 크리에이터)",
            "칸예 웨스트": "Kanye West (칸예 웨스트)",
            "트래비스 스캇": "Travis Scott (트래비스 스캇)",
            # ... 필요한 만큼 추가
        }
        
        # 아티스트 이름 변환
        def convert_artist_name(original_name):
            if original_name in artist_names:
                return artist_names[original_name]
            return original_name  # 매핑이 없으면 원본 유지
        
        df['artist'] = df['artist'].apply(convert_artist_name)
        
        # 정렬 (가나다순)
        df_sorted = df.sort_values('artist')
        
        print(f"총 {len(df_sorted)} 개의 콘서트 데이터 처리 완료")
        
        # 새로운 concerts.csv로 저장
        df_sorted.to_csv('/Users/youz2me/Xcode/Livith-Data/output/concerts_fixed.csv', 
                        index=False, encoding='utf-8')
        
        print("concerts_fixed.csv 파일로 저장 완료!")
        
        # 샘플 출력
        print("\n처리된 데이터 샘플:")
        print(df_sorted[['artist', 'title']].head(10))
        
    except Exception as e:
        print(f"오류 발생: {e}")

if __name__ == "__main__":
    fix_concerts_csv()