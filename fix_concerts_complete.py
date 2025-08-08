#!/usr/bin/env python3
import pandas as pd

def create_artist_mapping():
    """artists.csv에서 표준화된 아티스트 이름을 가져와서 매핑 생성"""
    try:
        artists_df = pd.read_csv('/Users/youz2me/Xcode/Livith-Data/output/artists.csv', 
                                encoding='utf-8', header=None)
        
        # 표준화된 아티스트 이름들 (한국어 (원어) 형식)
        standard_names = artists_df.iloc[:, 0].tolist()
        
        # 매핑 테이블 생성
        mapping = {}
        
        for name in standard_names:
            if '(' in name and ')' in name:
                # "한국어 (원어)" 형식에서 한국어 부분과 원어 부분 추출
                korean = name.split(' (')[0]
                original = name.split('(')[1].replace(')', '')
                
                # 다양한 변형들을 표준 이름으로 매핑
                mapping[korean] = name
                mapping[original] = name
                mapping[name] = name  # 이미 표준 형식인 경우
                
                # 특별 케이스들
                if korean == "히츠지분가쿠":
                    mapping["羊文学 (히츠지분가쿠)"] = name
                    mapping["羊文学"] = name
                elif korean == "사키야마 소우시":
                    mapping["사키야마소우시"] = name
                elif korean == "장학우":
                    mapping["장학우"] = name
                elif korean == "프레드 어게인":
                    mapping["프레드 어게인"] = name
                elif korean == "투홀리스":
                    mapping["2hollis"] = name
                elif korean == "예지":
                    mapping["예지"] = name
                elif korean == "호시노 겐":
                    mapping["호시노 겐"] = name
                elif korean == "알렉산드로스":
                    mapping["알렉산드로스"] = name
                    mapping["[Alexandros]"] = name
                    mapping["Alexandros"] = name
                elif korean == "히토리에":
                    mapping["히토리에"] = name
                elif korean == "챤미나":
                    mapping["CHANMINA"] = name
                elif korean == "스즈키 코노미":
                    mapping["스즈키 코노미"] = name
                elif korean == "칸예 웨스트":
                    mapping["칸예 웨스트"] = name
                elif korean == "트래비스 스캇":
                    mapping["트래비스 스캇"] = name
            else:
                # 괄호가 없는 경우 그대로 매핑
                mapping[name] = name
        
        return mapping
        
    except Exception as e:
        print(f"매핑 생성 중 오류: {e}")
        return {}

def fix_concerts_csv():
    try:
        print("step1_basic_concerts.csv 읽기...")
        df = pd.read_csv('/Users/youz2me/Xcode/Livith-Data/output/step1_basic_concerts.csv', 
                        encoding='utf-8')
        
        print("아티스트 매핑 생성...")
        mapping = create_artist_mapping()
        
        # 아티스트 이름 변환
        def convert_artist_name(original_name):
            if original_name in mapping:
                return mapping[original_name]
            else:
                print(f"매핑을 찾을 수 없는 아티스트: {original_name}")
                return original_name
        
        df['artist'] = df['artist'].apply(convert_artist_name)
        
        # 가나다순 정렬
        df_sorted = df.sort_values('artist')
        
        print(f"총 {len(df_sorted)}개의 콘서트 데이터 처리")
        
        # 새로운 concerts.csv 저장
        df_sorted.to_csv('/Users/youz2me/Xcode/Livith-Data/output/concerts.csv', 
                        index=False, encoding='utf-8')
        
        print("concerts.csv 복원 완료!")
        
        # 결과 확인
        print("\\n복원된 데이터 샘플:")
        print(df_sorted[['artist', 'title']].head(10))
        
        return True
        
    except Exception as e:
        print(f"복원 중 오류: {e}")
        return False

if __name__ == "__main__":
    success = fix_concerts_csv()
    if success:
        print("\\n✅ concerts.csv 파일이 성공적으로 복원되었습니다!")
    else:
        print("\\n❌ concerts.csv 복원에 실패했습니다.")