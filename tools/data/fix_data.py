#!/usr/bin/env python3
"""
데이터 수정 도구
- 아티스트명, 콘서트명 등 잘못된 정보 수정
- CSV 파일 수정 및 MySQL 데이터베이스 반영 옵션
- 연관 테이블 일괄 수정 기능

사용법:
1. 대화형 모드:
   python3 scripts/fix_data.py --interactive
   또는
   python3 scripts/fix_data.py

2. 검색 모드:
   python3 scripts/fix_data.py --search "제이콥" --type artist
   python3 scripts/fix_data.py --search "JVKE" --type concert

3. 환경 설정:
   - 테스트 모드: OUTPUT_MODE=test python3 scripts/fix_data.py
   - 프로덕션 모드: python3 scripts/fix_data.py

주요 기능:
- 🔍 데이터 검색: 아티스트명/콘서트명 부분 매칭 검색
- 🔄 CSV 수정: 로컬 CSV 파일들 일괄 업데이트 (자동 백업)
- 💾 MySQL 반영: 데이터베이스 직접 업데이트
- 🔗 연관 수정: 여러 CSV 파일의 관련 필드 동시 수정

예시 사용 사례:
- "제이콥 닷지 로슨" -> "JVKE" 아티스트명 수정
- concerts.csv, setlists.csv, songs.csv 등 모든 관련 파일에서 자동 수정
"""

import pandas as pd
import os
import subprocess
import mysql.connector
from mysql.connector import Error
import time
import signal
import argparse
from datetime import datetime
from typing import Dict, List, Optional
import json
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))
from lib.config import Config

class DataFixer:
    def __init__(self):
        self.ssh_process = None
        self.connection = None
        self.cursor = None
        # 출력 디렉토리 설정 (환경변수로 제어 가능)
        if os.environ.get('OUTPUT_MODE') == 'test':
            self.output_dir = str(Config.TEST_OUTPUT_DIR)
        else:
            self.output_dir = str(Config.OUTPUT_DIR)
        
        # CSV 파일들과 관련 컬럼 매핑
        self.csv_mappings = {
            'artist': {
                'concerts.csv': ['artist'],
                'artists.csv': ['artist'],
                'setlists.csv': ['artist'],  # setlists.csv uses 'artist' not 'artist_name'
                'songs.csv': ['artist'],
                'cultures.csv': ['artist_name'],
                'schedule.csv': ['artist_name'],
                'md.csv': ['artist_name'],
                'concert_info.csv': ['artist_name']
            },
            'concert_title': {
                'concerts.csv': ['title'],
                'setlists.csv': ['title'],  # setlists.csv uses 'title' not 'concert_title'
                'concert_setlists.csv': ['concert_title'],
                'cultures.csv': ['concert_title'],
                'schedule.csv': ['concert_title'],
                'md.csv': ['concert_title'],
                'concert_info.csv': ['concert_title']
            }
        }

    def show_menu(self):
        """메인 메뉴 출력"""
        print("\n" + "="*80)
        print("🔧 데이터 수정/삭제 도구")
        print("="*80)
        print("1. 아티스트명 수정")
        print("2. 콘서트명 수정") 
        print("3. 개별 필드 수정")
        print("4. 데이터 검색/확인")
        print("5. 🗑️  데이터 삭제")
        print("6. 종료")
        print("-"*80)

    def search_data(self, search_type: str, keyword: str) -> Dict:
        """데이터 검색"""
        results = {}
        
        if search_type == 'artist':
            mappings = self.csv_mappings['artist']
        elif search_type == 'concert':
            mappings = self.csv_mappings['concert_title']
        else:
            print("❌ 지원하지 않는 검색 타입입니다.")
            return results

        print(f"🔍 '{keyword}' 검색 중...")
        
        for csv_file, columns in mappings.items():
            csv_path = os.path.join(self.output_dir, csv_file)
            if not os.path.exists(csv_path):
                continue
                
            try:
                df = pd.read_csv(csv_path, encoding='utf-8-sig')
                df = df.fillna('')
                
                matches = []
                for col in columns:
                    if col in df.columns:
                        # 기본 검색 (부분 매칭)
                        mask = df[col].str.contains(keyword, case=False, na=False)
                        
                        # 유사 검색을 위한 추가 패턴들
                        similar_patterns = [
                            keyword.replace('손', '슨'),  # 손 <-> 슨
                            keyword.replace('슨', '손'),  # 슨 <-> 손  
                            keyword.replace('이', 'l'),   # 이 <-> l (예: 제이크 <-> 제lk)
                            keyword.replace('l', '이'),   # l <-> 이
                        ]
                        
                        # 추가 패턴들로도 검색
                        for pattern in similar_patterns:
                            if pattern != keyword:  # 원래 키워드와 다를 때만
                                additional_mask = df[col].str.contains(pattern, case=False, na=False)
                                mask = mask | additional_mask
                        
                        if mask.any():
                            matched_rows = df[mask]
                            for _, row in matched_rows.iterrows():
                                matches.append({
                                    'column': col,
                                    'value': row[col],
                                    'row_data': dict(row)
                                })
                
                if matches:
                    results[csv_file] = matches
                    
            except Exception as e:
                print(f"⚠️ {csv_file} 읽기 실패: {e}")
        
        return results

    def show_search_results(self, results: Dict, keyword: str):
        """검색 결과 출력"""
        if not results:
            print(f"❌ '{keyword}'에 대한 검색 결과가 없습니다.")
            
            # 유사한 이름 제안
            print(f"\n💡 혹시 다음 중 하나를 찾고 계신가요?")
            suggestions = []
            
            # CSV 파일들에서 유사한 이름 찾기
            for csv_file in ['concerts.csv', 'artists.csv']:
                csv_path = os.path.join(self.output_dir, csv_file)
                if os.path.exists(csv_path):
                    try:
                        df = pd.read_csv(csv_path, encoding='utf-8-sig')
                        df = df.fillna('')
                        
                        # artist 관련 컬럼들 확인
                        artist_columns = ['artist', 'artist_name'] if csv_file == 'concerts.csv' else ['artist']
                        for col in artist_columns:
                            if col in df.columns:
                                unique_artists = df[col].unique()
                                for artist in unique_artists:
                                    if artist and len(artist.strip()) > 0:
                                        suggestions.append(artist.strip())
                    except:
                        pass
            
            # 중복 제거하고 제안
            unique_suggestions = list(set(suggestions))[:5]  # 최대 5개만
            for i, suggestion in enumerate(unique_suggestions, 1):
                print(f"  {i}. {suggestion}")
            
            return False
            
        print(f"\n📋 '{keyword}' 검색 결과:")
        print("-"*80)
        
        total_matches = 0
        for csv_file, matches in results.items():
            print(f"\n📄 {csv_file}: {len(matches)}개 발견")
            for i, match in enumerate(matches[:5]):  # 최대 5개만 표시
                print(f"  {i+1}. {match['column']}: {match['value']}")
                if 'title' in match['row_data']:
                    print(f"     Title: {match['row_data']['title']}")
            
            if len(matches) > 5:
                print(f"     ... 외 {len(matches)-5}개 더")
            total_matches += len(matches)
        
        print(f"\n총 {total_matches}개의 매칭 항목 발견")
        return True

    def update_csv_files(self, update_type: str, old_value: str, new_value: str, target_files: List[str] = None) -> Dict:
        """CSV 파일들 업데이트"""
        if update_type == 'artist':
            mappings = self.csv_mappings['artist']
        elif update_type == 'concert_title':
            mappings = self.csv_mappings['concert_title']
        else:
            print("❌ 지원하지 않는 업데이트 타입입니다.")
            return {}

        results = {}
        
        # 백업 디렉토리 생성
        backup_dir = os.path.join(self.output_dir, 'backups', datetime.now().strftime('%Y%m%d_%H%M%S'))
        os.makedirs(backup_dir, exist_ok=True)
        
        files_to_process = target_files if target_files else mappings.keys()
        
        for csv_file in files_to_process:
            if csv_file not in mappings:
                continue
                
            csv_path = os.path.join(self.output_dir, csv_file)
            if not os.path.exists(csv_path):
                results[csv_file] = {'status': 'not_found', 'updated': 0}
                continue
            
            try:
                # 백업 생성
                backup_path = os.path.join(backup_dir, csv_file)
                df = pd.read_csv(csv_path, encoding='utf-8-sig')
                df.to_csv(backup_path, index=False, encoding='utf-8-sig')
                
                # 데이터 업데이트
                df = df.fillna('')
                updated_count = 0
                columns = mappings[csv_file]
                
                for col in columns:
                    if col in df.columns:
                        mask = df[col] == old_value
                        if mask.any():
                            df.loc[mask, col] = new_value
                            updated_count += mask.sum()
                
                if updated_count > 0:
                    # 업데이트된 CSV 저장
                    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                    results[csv_file] = {'status': 'updated', 'updated': updated_count}
                    print(f"✅ {csv_file}: {updated_count}개 항목 업데이트")
                else:
                    results[csv_file] = {'status': 'no_matches', 'updated': 0}
                    print(f"ℹ️ {csv_file}: 매칭 항목 없음")
                    
            except Exception as e:
                results[csv_file] = {'status': 'error', 'error': str(e), 'updated': 0}
                print(f"❌ {csv_file} 업데이트 실패: {e}")
        
        print(f"\n📋 백업 생성됨: {backup_dir}")
        return results

    def create_ssh_tunnel(self):
        """SSH 터널 생성"""
        try:
            print("🔧 SSH 터널 생성 중...")
            
            ssh_command = [
                'ssh',
                '-i', Config.get_ssh_key_path(),
                '-L', '3307:livithdb.c142i2022qs5.ap-northeast-2.rds.amazonaws.com:3306',
                '-N',
                '-o', 'StrictHostKeyChecking=no',
                'ubuntu@43.203.48.65'
            ]
            
            self.ssh_process = subprocess.Popen(
                ssh_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid
            )
            
            time.sleep(3)
            
            if self.ssh_process.poll() is None:
                print("✅ SSH 터널 생성 완료!")
                return True
            else:
                print("❌ SSH 터널 생성 실패")
                return False
                
        except Exception as e:
            print(f"❌ SSH 터널 오류: {e}")
            return False

    def connect_mysql(self):
        """MySQL 연결"""
        try:
            print("🔌 MySQL 연결 중...")
            
            config = {
                'host': '127.0.0.1',
                'port': 3307,
                'user': 'root',
                'password': 'livith0407',
                'database': 'livith_v2',
                'charset': 'utf8mb4',
                'use_unicode': True
            }
            
            self.connection = mysql.connector.connect(**config)
            self.cursor = self.connection.cursor()
            
            print("✅ MySQL 연결 성공!")
            return True
            
        except Error as e:
            print(f"❌ MySQL 연결 실패: {e}")
            return False

    def update_mysql_data(self, update_type: str, old_value: str, new_value: str) -> Dict:
        """MySQL 데이터베이스 업데이트"""
        if not self.connection or not self.cursor:
            print("❌ MySQL 연결이 필요합니다.")
            return {}

        results = {}
        
        try:
            if update_type == 'artist':
                # 아티스트 관련 테이블들 업데이트
                tables_queries = {
                    'artists': "UPDATE artists SET artist = %s WHERE artist = %s",
                    'concerts': "UPDATE concerts SET artist = %s WHERE artist = %s",
                    # 필요시 다른 테이블들도 추가
                }
            elif update_type == 'concert_title':
                # 콘서트 제목 관련 테이블들 업데이트  
                tables_queries = {
                    'concerts': "UPDATE concerts SET title = %s WHERE title = %s",
                    # 필요시 다른 테이블들도 추가
                }
            else:
                print("❌ 지원하지 않는 업데이트 타입입니다.")
                return {}

            for table, query in tables_queries.items():
                try:
                    self.cursor.execute(query, (new_value, old_value))
                    affected_rows = self.cursor.rowcount
                    results[table] = {'status': 'updated', 'affected_rows': affected_rows}
                    print(f"✅ {table}: {affected_rows}개 행 업데이트")
                except Exception as e:
                    results[table] = {'status': 'error', 'error': str(e)}
                    print(f"❌ {table} 업데이트 실패: {e}")
            
            # 변경사항 커밋
            self.connection.commit()
            print("✅ MySQL 변경사항 커밋 완료")
            
        except Exception as e:
            print(f"❌ MySQL 업데이트 실패: {e}")
            self.connection.rollback()
            
        return results

    def interactive_fix_artist(self):
        """대화형 아티스트명 수정"""
        print("\n" + "="*60)
        print("🎤 아티스트명 수정")
        print("="*60)
        
        # 검색
        keyword = input("검색할 아티스트명 입력 (부분 검색 가능): ").strip()
        if not keyword:
            print("❌ 검색어를 입력해주세요.")
            return
        
        results = self.search_data('artist', keyword)
        if not self.show_search_results(results, keyword):
            return
        
        # 수정할 값 입력
        print("\n" + "-"*60)
        old_value = input("수정할 기존 아티스트명 (정확히): ").strip()
        new_value = input("새로운 아티스트명: ").strip()
        
        if not old_value or not new_value:
            print("❌ 기존 값과 새 값을 모두 입력해주세요.")
            return
        
        # 수정 범위 선택
        print(f"\n📋 '{old_value}' -> '{new_value}' 수정 진행")
        print("1. CSV 파일만 수정")
        print("2. MySQL 데이터베이스만 수정") 
        print("3. CSV + MySQL 모두 수정")
        
        choice = input("선택 (1-3): ").strip()
        
        if choice in ['1', '3']:
            print(f"\n🔄 CSV 파일 수정 중...")
            csv_results = self.update_csv_files('artist', old_value, new_value)
        
        if choice in ['2', '3']:
            print(f"\n🔄 MySQL 데이터베이스 수정 중...")
            if not self.connection:
                if self.create_ssh_tunnel() and self.connect_mysql():
                    mysql_results = self.update_mysql_data('artist', old_value, new_value)
                else:
                    print("❌ MySQL 연결 실패")
            else:
                mysql_results = self.update_mysql_data('artist', old_value, new_value)

    def interactive_fix_concert(self):
        """대화형 콘서트명 수정"""
        print("\n" + "="*60)
        print("🎵 콘서트명 수정")
        print("="*60)
        
        # 검색
        keyword = input("검색할 콘서트명 입력 (부분 검색 가능): ").strip()
        if not keyword:
            print("❌ 검색어를 입력해주세요.")
            return
        
        results = self.search_data('concert', keyword)
        if not self.show_search_results(results, keyword):
            return
        
        # 수정할 값 입력
        print("\n" + "-"*60)
        old_value = input("수정할 기존 콘서트명 (정확히): ").strip()
        new_value = input("새로운 콘서트명: ").strip()
        
        if not old_value or not new_value:
            print("❌ 기존 값과 새 값을 모두 입력해주세요.")
            return
        
        # 수정 범위 선택
        print(f"\n📋 '{old_value}' -> '{new_value}' 수정 진행")
        print("1. CSV 파일만 수정")
        print("2. MySQL 데이터베이스만 수정")
        print("3. CSV + MySQL 모두 수정")
        
        choice = input("선택 (1-3): ").strip()
        
        if choice in ['1', '3']:
            print(f"\n🔄 CSV 파일 수정 중...")
            csv_results = self.update_csv_files('concert_title', old_value, new_value)
        
        if choice in ['2', '3']:
            print(f"\n🔄 MySQL 데이터베이스 수정 중...")
            if not self.connection:
                if self.create_ssh_tunnel() and self.connect_mysql():
                    mysql_results = self.update_mysql_data('concert_title', old_value, new_value)
                else:
                    print("❌ MySQL 연결 실패")
            else:
                mysql_results = self.update_mysql_data('concert_title', old_value, new_value)

    def delete_data_menu(self):
        """데이터 삭제 메뉴"""
        print("\n" + "="*60)
        print("🗑️ 데이터 삭제")
        print("="*60)
        print("1. 아티스트 삭제 (관련 모든 데이터 삭제)")
        print("2. 콘서트 삭제 (콘서트 관련 데이터만 삭제)")
        print("3. 취소")
        
        choice = input("선택 (1-3): ").strip()
        
        if choice == '1':
            self.delete_artist()
        elif choice == '2':
            self.delete_concert()
        elif choice == '3':
            print("❌ 삭제 취소")
            return
        else:
            print("❌ 잘못된 선택입니다.")
    
    def delete_artist(self):
        """아티스트 및 관련 모든 데이터 삭제"""
        print("\n" + "="*60)
        print("🎤 아티스트 삭제 (⚠️ 모든 관련 데이터가 삭제됩니다)")
        print("="*60)
        
        # 아티스트 검색
        keyword = input("삭제할 아티스트명 검색: ").strip()
        if not keyword:
            print("❌ 검색어를 입력해주세요.")
            return
        
        results = self.search_data('artist', keyword)
        if not self.show_search_results(results, keyword):
            return
        
        # 삭제할 아티스트 확인
        artist_name = input("\n삭제할 아티스트명 (정확히 입력): ").strip()
        if not artist_name:
            print("❌ 아티스트명을 입력해주세요.")
            return
        
        # 삭제 확인
        print(f"\n⚠️  경고: '{artist_name}' 아티스트와 관련된 모든 데이터가 삭제됩니다:")
        print("  - concerts.csv의 해당 아티스트 콘서트")
        print("  - artists.csv의 아티스트 정보")
        print("  - songs.csv의 해당 아티스트 곡")
        print("  - setlists.csv의 관련 셋리스트")
        print("  - cultures.csv, schedule.csv, md.csv, concert_info.csv의 관련 데이터")
        
        confirm = input("\n정말 삭제하시겠습니까? (yes/no): ").strip().lower()
        if confirm != 'yes':
            print("❌ 삭제 취소됨")
            return
        
        # 백업 디렉토리 생성
        backup_dir = os.path.join(self.output_dir, 'backups', datetime.now().strftime('%Y%m%d_%H%M%S'))
        os.makedirs(backup_dir, exist_ok=True)
        
        # 삭제 작업
        deleted_stats = {}
        
        # 1. concerts.csv에서 해당 아티스트의 콘서트 찾기
        concerts_to_delete = []
        csv_path = os.path.join(self.output_dir, 'concerts.csv')
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            backup_path = os.path.join(backup_dir, 'concerts.csv')
            df.to_csv(backup_path, index=False, encoding='utf-8-sig')
            
            concerts_df = df[df['artist'] == artist_name]
            concerts_to_delete = concerts_df['title'].tolist()
            
            df = df[df['artist'] != artist_name]
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            deleted_stats['concerts'] = len(concerts_to_delete)
        
        # 2. 콘서트 관련 데이터 삭제
        for concert_title in concerts_to_delete:
            self._delete_concert_data(concert_title, backup_dir)
        
        # 3. artists.csv에서 아티스트 삭제
        csv_path = os.path.join(self.output_dir, 'artists.csv')
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            backup_path = os.path.join(backup_dir, 'artists.csv')
            df.to_csv(backup_path, index=False, encoding='utf-8-sig')
            
            before_count = len(df)
            df = df[df['artist'] != artist_name]
            after_count = len(df)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            deleted_stats['artists'] = before_count - after_count
        
        # 4. songs.csv에서 아티스트 곡 삭제
        csv_path = os.path.join(self.output_dir, 'songs.csv')
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            backup_path = os.path.join(backup_dir, 'songs.csv')
            df.to_csv(backup_path, index=False, encoding='utf-8-sig')
            
            before_count = len(df)
            df = df[df['artist'] != artist_name]
            after_count = len(df)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            deleted_stats['songs'] = before_count - after_count
        
        # 결과 출력
        print(f"\n✅ 아티스트 '{artist_name}' 삭제 완료!")
        print("삭제된 데이터:")
        for key, count in deleted_stats.items():
            if count > 0:
                print(f"  - {key}: {count}개")
        print(f"\n📋 백업 생성됨: {backup_dir}")
    
    def delete_concert(self):
        """특정 콘서트 및 관련 데이터 삭제"""
        print("\n" + "="*60)
        print("🎵 콘서트 삭제")
        print("="*60)
        
        # 아티스트로 먼저 검색
        keyword = input("아티스트명 검색: ").strip()
        if not keyword:
            print("❌ 검색어를 입력해주세요.")
            return
        
        # 해당 아티스트의 콘서트 목록 보여주기
        csv_path = os.path.join(self.output_dir, 'concerts.csv')
        if not os.path.exists(csv_path):
            print("❌ concerts.csv 파일이 없습니다.")
            return
        
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        df = df.fillna('')
        
        # 아티스트명으로 필터링
        mask = df['artist'].str.contains(keyword, case=False, na=False)
        filtered_df = df[mask]
        
        if filtered_df.empty:
            print(f"❌ '{keyword}' 아티스트의 콘서트를 찾을 수 없습니다.")
            return
        
        # 콘서트 목록 출력
        print(f"\n📋 '{keyword}' 관련 콘서트 목록:")
        print("-"*80)
        for idx, (index, row) in enumerate(filtered_df.iterrows(), 1):
            print(f"{idx}. {row['title']} ({row['start_date']}) - {row['venue']}")
        
        # 삭제할 콘서트 선택
        try:
            choice = int(input("\n삭제할 콘서트 번호 선택: "))
            if choice < 1 or choice > len(filtered_df):
                print("❌ 잘못된 번호입니다.")
                return
        except ValueError:
            print("❌ 숫자를 입력해주세요.")
            return
        
        selected_concert = filtered_df.iloc[choice - 1]
        concert_title = selected_concert['title']
        
        # 삭제 확인
        print(f"\n⚠️  경고: '{concert_title}' 콘서트와 관련된 데이터가 삭제됩니다:")
        print("  - concerts.csv의 콘서트 정보")
        print("  - setlists.csv, concert_setlists.csv의 셋리스트")
        print("  - cultures.csv, schedule.csv, md.csv, concert_info.csv의 관련 데이터")
        
        confirm = input("\n정말 삭제하시겠습니까? (yes/no): ").strip().lower()
        if confirm != 'yes':
            print("❌ 삭제 취소됨")
            return
        
        # 백업 디렉토리 생성
        backup_dir = os.path.join(self.output_dir, 'backups', datetime.now().strftime('%Y%m%d_%H%M%S'))
        os.makedirs(backup_dir, exist_ok=True)
        
        # 삭제 작업
        deleted_stats = {}
        
        # 1. concerts.csv에서 콘서트 삭제
        backup_path = os.path.join(backup_dir, 'concerts.csv')
        df.to_csv(backup_path, index=False, encoding='utf-8-sig')
        
        df = df[df['title'] != concert_title]
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        deleted_stats['concerts'] = 1
        
        # 2. 콘서트 관련 데이터 삭제
        self._delete_concert_data(concert_title, backup_dir)
        
        print(f"\n✅ 콘서트 '{concert_title}' 삭제 완료!")
        print(f"📋 백업 생성됨: {backup_dir}")
    
    def _delete_concert_data(self, concert_title: str, backup_dir: str):
        """콘서트 관련 데이터 삭제 (내부 함수)"""
        # 삭제할 CSV 파일들과 컬럼 매핑
        concert_related_files = {
            'setlists.csv': 'title',  # setlists.csv uses 'title' not 'concert_title'
            'concert_setlists.csv': 'concert_title',
            'cultures.csv': 'concert_title',
            'schedule.csv': 'concert_title',
            'md.csv': 'concert_title',
            'concert_info.csv': 'concert_title',
            'concert_genres.csv': 'concert_title'
        }
        
        for csv_file, column in concert_related_files.items():
            csv_path = os.path.join(self.output_dir, csv_file)
            if os.path.exists(csv_path):
                try:
                    df = pd.read_csv(csv_path, encoding='utf-8-sig')
                    df = df.fillna('')
                    
                    # 백업
                    backup_path = os.path.join(backup_dir, csv_file)
                    df.to_csv(backup_path, index=False, encoding='utf-8-sig')
                    
                    # 삭제
                    if column in df.columns:
                        before_count = len(df)
                        df = df[df[column] != concert_title]
                        after_count = len(df)
                        if before_count > after_count:
                            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                            print(f"✅ {csv_file}: {before_count - after_count}개 항목 삭제됨")
                    else:
                        print(f"⚠️ {csv_file}: '{column}' 컬럼을 찾을 수 없음")
                        
                except Exception as e:
                    print(f"❌ {csv_file} 처리 중 오류: {e}")
            else:
                print(f"ℹ️ {csv_file}: 파일이 존재하지 않음")
        
        # setlist_songs.csv 처리 (setlists를 통해 간접 삭제)
        setlists_path = os.path.join(self.output_dir, 'setlists.csv')
        setlist_songs_path = os.path.join(self.output_dir, 'setlist_songs.csv')
        
        if os.path.exists(setlists_path) and os.path.exists(setlist_songs_path):
            try:
                # 삭제될 셋리스트 title 찾기 (setlists.csv에서 title 컬럼 사용)
                setlists_df = pd.read_csv(setlists_path, encoding='utf-8-sig')
                
                # 콘서트 제목과 매칭되는 셋리스트 찾기 (보통 "[콘서트제목] 예상 셋리스트" 형태)
                deleted_setlist_titles = []
                for _, row in setlists_df.iterrows():
                    if concert_title in str(row['title']):  # 콘서트 제목이 셋리스트 제목에 포함되어 있는지 확인
                        deleted_setlist_titles.append(row['title'])
                
                if deleted_setlist_titles:
                    # setlist_songs에서 해당 셋리스트 title 삭제
                    df = pd.read_csv(setlist_songs_path, encoding='utf-8-sig')
                    backup_path = os.path.join(backup_dir, 'setlist_songs.csv')
                    df.to_csv(backup_path, index=False, encoding='utf-8-sig')
                    
                    before_count = len(df)
                    df = df[~df['setlist_title'].isin(deleted_setlist_titles)]
                    after_count = len(df)
                    
                    if before_count > after_count:
                        df.to_csv(setlist_songs_path, index=False, encoding='utf-8-sig')
                        print(f"✅ setlist_songs.csv: {before_count - after_count}개 곡 삭제됨")
                    else:
                        print(f"ℹ️ setlist_songs.csv: 삭제할 곡이 없음")
                else:
                    print(f"ℹ️ 콘서트 '{concert_title}'와 매칭되는 셋리스트 없음")
                    
            except Exception as e:
                print(f"❌ setlist_songs.csv 처리 중 오류: {e}")
        else:
            if not os.path.exists(setlists_path):
                print(f"ℹ️ setlists.csv: 파일이 존재하지 않음")
            if not os.path.exists(setlist_songs_path):
                print(f"ℹ️ setlist_songs.csv: 파일이 존재하지 않음")
    
    def interactive_search(self):
        """대화형 데이터 검색"""
        print("\n" + "="*60)
        print("🔍 데이터 검색")
        print("="*60)
        
        print("1. 아티스트 검색")
        print("2. 콘서트 검색")
        
        choice = input("선택 (1-2): ").strip()
        keyword = input("검색어 입력: ").strip()
        
        if not keyword:
            print("❌ 검색어를 입력해주세요.")
            return
        
        if choice == '1':
            results = self.search_data('artist', keyword)
            self.show_search_results(results, keyword)
        elif choice == '2':
            results = self.search_data('concert', keyword)
            self.show_search_results(results, keyword)
        else:
            print("❌ 잘못된 선택입니다.")

    def close(self):
        """연결 종료"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        if self.ssh_process:
            try:
                os.killpg(os.getpgid(self.ssh_process.pid), signal.SIGTERM)
                self.ssh_process.wait(timeout=5)
            except:
                try:
                    os.killpg(os.getpgid(self.ssh_process.pid), signal.SIGKILL)
                except:
                    pass
        print("🔌 연결 종료")

    def run(self):
        """메인 실행 루프"""
        try:
            while True:
                self.show_menu()
                choice = input("선택하세요 (1-6): ").strip()
                
                if choice == '1':
                    self.interactive_fix_artist()
                elif choice == '2':
                    self.interactive_fix_concert()
                elif choice == '3':
                    print("⚠️ 개별 필드 수정 기능은 추후 구현 예정입니다.")
                elif choice == '4':
                    self.interactive_search()
                elif choice == '5':
                    self.delete_data_menu()
                elif choice == '6':
                    print("👋 프로그램을 종료합니다.")
                    break
                else:
                    print("❌ 잘못된 선택입니다. 1-6 중에서 선택해주세요.")
                
                input("\nEnter를 눌러 계속...")
                
        except KeyboardInterrupt:
            print("\n⏹️ 사용자 중단")
        except Exception as e:
            print(f"❌ 오류 발생: {e}")
        finally:
            self.close()

def main():
    """커맨드라인 인터페이스"""
    parser = argparse.ArgumentParser(description='데이터 수정 도구')
    parser.add_argument('--interactive', '-i', action='store_true', help='대화형 모드 실행')
    parser.add_argument('--search', '-s', help='검색어')
    parser.add_argument('--type', '-t', choices=['artist', 'concert'], help='검색 타입')
    
    args = parser.parse_args()
    fixer = DataFixer()
    
    if args.interactive or (not args.search and not args.type):
        # 대화형 모드
        fixer.run()
    elif args.search and args.type:
        # 커맨드라인 검색
        results = fixer.search_data(args.type, args.search)
        fixer.show_search_results(results, args.search)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()