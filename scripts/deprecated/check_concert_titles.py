#!/usr/bin/env python3
import pandas as pd
import os

def check_concert_titles():
    base_path = '/Users/youz2me/Xcode/Livith-Data/output/'
    
    # concerts.csv를 기준으로 title 가져오기
    print("=" * 60)
    print("1. concerts.csv 파일 읽기...")
    concerts_df = pd.read_csv(base_path + 'concerts.csv', encoding='utf-8')
    concerts_titles = set(concerts_df['title'].tolist())
    print(f"concerts.csv의 title 개수: {len(concerts_titles)}")
    
    results = {}
    
    # concert_info.csv 체크
    print("\n" + "=" * 60)
    print("2. concert_info.csv 파일 확인...")
    try:
        concert_info_df = pd.read_csv(base_path + 'concert_info.csv', encoding='utf-8')
        # 첫 번째 컬럼이 concert_title
        concert_info_titles = set(concert_info_df.iloc[:, 0].tolist())
        
        only_in_concert_info = concert_info_titles - concerts_titles
        only_in_concerts = concerts_titles - concert_info_titles
        matched = concert_info_titles & concerts_titles
        
        print(f"concert_info.csv의 title 개수: {len(concert_info_titles)}")
        print(f"매칭된 title: {len(matched)}개")
        
        results['concert_info'] = {
            'total': len(concert_info_titles),
            'matched': len(matched),
            'only_here': only_in_concert_info,
            'missing_from_here': only_in_concerts
        }
    except Exception as e:
        print(f"오류: {e}")
        results['concert_info'] = None
    
    # concert_setlists.csv 체크
    print("\n" + "=" * 60)
    print("3. concert_setlists.csv 파일 확인...")
    try:
        concert_setlists_df = pd.read_csv(base_path + 'concert_setlists.csv', encoding='utf-8')
        # 첫 번째 컬럼이 concert_title
        concert_setlists_titles = set(concert_setlists_df.iloc[:, 0].tolist())
        
        only_in_setlists = concert_setlists_titles - concerts_titles
        only_in_concerts = concerts_titles - concert_setlists_titles
        matched = concert_setlists_titles & concerts_titles
        
        print(f"concert_setlists.csv의 title 개수: {len(concert_setlists_titles)}")
        print(f"매칭된 title: {len(matched)}개")
        
        results['concert_setlists'] = {
            'total': len(concert_setlists_titles),
            'matched': len(matched),
            'only_here': only_in_setlists,
            'missing_from_here': only_in_concerts
        }
    except Exception as e:
        print(f"오류: {e}")
        results['concert_setlists'] = None
    
    # cultures.csv 체크
    print("\n" + "=" * 60)
    print("4. cultures.csv 파일 확인...")
    try:
        cultures_df = pd.read_csv(base_path + 'cultures.csv', encoding='utf-8')
        # 첫 번째 컬럼이 concert_title인지 확인 필요
        cultures_titles = set(cultures_df.iloc[:, 0].tolist())
        
        only_in_cultures = cultures_titles - concerts_titles
        only_in_concerts = concerts_titles - cultures_titles
        matched = cultures_titles & concerts_titles
        
        print(f"cultures.csv의 title 개수: {len(cultures_titles)}")
        print(f"매칭된 title: {len(matched)}개")
        
        results['cultures'] = {
            'total': len(cultures_titles),
            'matched': len(matched),
            'only_here': only_in_cultures,
            'missing_from_here': only_in_concerts
        }
    except Exception as e:
        print(f"오류: {e}")
        results['cultures'] = None
    
    # schedule.csv 체크
    print("\n" + "=" * 60)
    print("5. schedule.csv 파일 확인...")
    schedule_path = base_path + 'schedule.csv'
    if os.path.exists(schedule_path):
        try:
            schedule_df = pd.read_csv(schedule_path, encoding='utf-8')
            # concert_title 컬럼 있는지 확인
            if 'concert_title' in schedule_df.columns:
                schedule_titles = set(schedule_df['concert_title'].tolist())
            else:
                # 첫 번째 컬럼 사용
                schedule_titles = set(schedule_df.iloc[:, 0].tolist())
            
            only_in_schedule = schedule_titles - concerts_titles
            only_in_concerts = concerts_titles - schedule_titles
            matched = schedule_titles & concerts_titles
            
            print(f"schedule.csv의 title 개수: {len(schedule_titles)}")
            print(f"매칭된 title: {len(matched)}개")
            
            results['schedule'] = {
                'total': len(schedule_titles),
                'matched': len(matched),
                'only_here': only_in_schedule,
                'missing_from_here': only_in_concerts
            }
        except Exception as e:
            print(f"오류: {e}")
            results['schedule'] = None
    else:
        print("schedule.csv 파일이 존재하지 않습니다.")
        results['schedule'] = None
    
    # 결과 출력
    print("\n" + "=" * 60)
    print("📊 최종 분석 결과")
    print("=" * 60)
    
    for file_name, result in results.items():
        if result:
            print(f"\n📁 {file_name}.csv:")
            print(f"  - 전체: {result['total']}개")
            print(f"  - 매칭: {result['matched']}개")
            
            if result['only_here']:
                print(f"  - {file_name}에만 있는 title ({len(result['only_here'])}개):")
                for title in sorted(list(result['only_here']))[:10]:  # 처음 10개만
                    print(f"    • {title}")
                if len(result['only_here']) > 10:
                    print(f"    ... 외 {len(result['only_here']) - 10}개")
            
            if result['missing_from_here']:
                print(f"  - concerts.csv에만 있는 title ({len(result['missing_from_here'])}개):")
                for title in sorted(list(result['missing_from_here']))[:10]:  # 처음 10개만
                    print(f"    • {title}")
                if len(result['missing_from_here']) > 10:
                    print(f"    ... 외 {len(result['missing_from_here']) - 10}개")

if __name__ == "__main__":
    check_concert_titles()