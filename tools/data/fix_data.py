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
import re
import subprocess
import mysql.connector
from mysql.connector import Error
import time
import signal
import argparse
import shutil
from datetime import datetime
from typing import Dict, List, Optional
import json
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))
from lib.config import Config
from lib.db_utils import get_db_manager, get_dev_db_manager, get_stage_db_manager

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
        print("5. 데이터 삭제")
        print("6. 아티스트명 형식 일괄 교정 (영문 (한국어))")
        print("7. 종료")
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
                        mask = df[col].str.contains(keyword, case=False, na=False, regex=False)
                        
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
                                additional_mask = df[col].str.contains(pattern, case=False, na=False, regex=False)
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

    def _connect_db(self, db):
        """db 매니저로 연결 후 self.connection/cursor 설정"""
        if db.connect_with_ssh():
            self.connection = db.connection
            self.cursor = db.cursor
            return True
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
            print("  1. Dev DB  2. 프로덕션 DB  3. Stage DB")
            db_choice = input("  DB 선택 (1/2/3): ").strip()
            db_map = {'1': get_dev_db_manager, '2': get_db_manager, '3': get_stage_db_manager}
            db_factory = db_map.get(db_choice, get_db_manager)
            db = db_factory()
            if self._connect_db(db):
                mysql_results = self.update_mysql_data('artist', old_value, new_value)
                db.disconnect()
            else:
                print("❌ MySQL 연결 실패")

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
            print("  1. Dev DB  2. 프로덕션 DB  3. Stage DB")
            db_choice = input("  DB 선택 (1/2/3): ").strip()
            db_map = {'1': get_dev_db_manager, '2': get_db_manager, '3': get_stage_db_manager}
            db_factory = db_map.get(db_choice, get_db_manager)
            db = db_factory()
            if self._connect_db(db):
                mysql_results = self.update_mysql_data('concert_title', old_value, new_value)
                db.disconnect()
            else:
                print("❌ MySQL 연결 실패")

    def interactive_fix_individual_fields(self):
        """개별 필드 수정 메뉴"""
        while True:
            print("\n" + "="*60)
            print("📝 개별 필드 수정")
            print("="*60)
            print("1. 빈 콘서트에 아티스트 작성")
            print("2. 신규 아티스트 추가")
            print("3. artists.csv 아티스트 정보 수정")
            print("4. artists.csv 아티스트명 수정")
            print("5. concerts.csv 아티스트명 수정")
            print("6. songs.csv 노래 제목 수정")
            print("7. songs.csv 가사 추가")
            print("8. 돌아가기")
            
            choice = input("선택 (1-8): ").strip()
            
            if choice == '1':
                self.fill_missing_artists()
            elif choice == '2':
                self.add_new_artists()
            elif choice == '3':
                self.update_artist_info_from_csv()
            elif choice == '4':
                self.edit_artist_in_artists_csv()
            elif choice == '5':
                self.edit_artist_in_concerts_csv()
            elif choice == '6':
                self.edit_song_title()
            elif choice == '7':
                self.add_lyrics_to_songs()
            elif choice == '8':
                break
            else:
                print("❌ 잘못된 선택입니다.")

    def update_artist_info_from_csv(self):
        """artists.csv의 아티스트 정보를 다시 수집합니다."""
        print("\n" + "="*60)
        print("🔄 artist.csv 아티스트 정보 수정")
        print("="*60)
        artist_name = input("정보를 수정할 아티스트의 정확한 이름을 입력하세요: ").strip()
        if not artist_name:
            print("❌ 아티스트 이름이 입력되지 않았습니다.")
            return

        script_path = Path(__file__).parent / 'update_artist_basic_info.py'
        command = [sys.executable, str(script_path), "--artist", artist_name]

        try:
            print(f"🚀 '{artist_name}'의 정보 업데이트를 시작합니다...")
            subprocess.run(command, check=True)
            print(f"✅ '{artist_name}'의 정보 업데이트가 완료되었습니다.")
        except subprocess.CalledProcessError as e:
            print(f"❌ 스크립트 실행 중 오류가 발생했습니다: {e}")
        except FileNotFoundError:
            print(f"❌ 스크립트 파일을 찾을 수 없습니다: {script_path}")

    def edit_artist_in_concerts_csv(self):
        """concerts.csv에서 특정 콘서트의 아티스트명을 수정합니다."""
        csv_path = os.path.join(self.output_dir, 'concerts.csv')
        if not os.path.exists(csv_path):
            print("❌ concerts.csv 파일을 찾을 수 없습니다.")
            return

        try:
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            df['artist'] = df['artist'].fillna('')

            while True:
                print("\n" + "-"*60)
                print("🎤 수정할 콘서트를 선택하세요")
                print("-"*60)

                for i, row in df.iterrows():
                    print(f"{i+1}. {row['title']} (현재 아티스트: {row['artist']})")

                print("\n'q'를 입력하여 종료")
                choice = input("수정할 콘서트 번호를 선택하세요: ").strip()

                if choice.lower() == 'q':
                    break

                try:
                    choice_idx = int(choice) - 1
                    if not (0 <= choice_idx < len(df)):
                        print("❌ 잘못된 번호입니다.")
                        continue

                    old_artist = df.loc[choice_idx, 'artist']
                    concert_title = df.loc[choice_idx, 'title']
                    new_artist = input(f"'{concert_title}'의 새 아티스트명을 입력하세요 (현재: {old_artist}): ").strip()

                    if not new_artist:
                        print("❌ 새 아티스트명이 입력되지 않았습니다. 취소합니다.")
                        continue

                    # DataFrame 업데이트
                    df.loc[choice_idx, 'artist'] = new_artist

                    # CSV 파일 저장
                    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                    print(f"✅ '{concert_title}'의 아티스트가 '{new_artist}'(으)로 성공적으로 변경되었습니다.")
                    break

                except ValueError:
                    print("❌ 숫자를 입력하거나 'q'를 입력하여 종료하세요.")
                except Exception as e:
                    print(f"❌ 업데이트 중 오류 발생: {e}")

        except Exception as e:
            print(f"❌ 파일 처리 중 오류 발생: {e}")

    def edit_artist_in_artists_csv(self):
        """artists.csv에서 아티스트명을 직접 수정합니다."""
        csv_path = os.path.join(self.output_dir, 'artists.csv')
        if not os.path.exists(csv_path):
            print("❌ artists.csv 파일을 찾을 수 없습니다.")
            return

        try:
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            df['artist'] = df['artist'].fillna('')

            while True:
                print("\n" + "-"*60)
                print("🎤 수정할 아티스트를 선택하세요")
                print("-"*60)

                artists = df['artist'].tolist()
                for i, artist_name in enumerate(artists):
                    print(f"{i+1}. {artist_name}")

                print("\n'q'를 입력하여 종료")
                choice = input("수정할 아티스트 번호를 선택하세요: ").strip()

                if choice.lower() == 'q':
                    break

                try:
                    choice_idx = int(choice) - 1
                    if not (0 <= choice_idx < len(artists)):
                        print("❌ 잘못된 번호입니다.")
                        continue

                    old_name = artists[choice_idx]
                    new_name = input(f"'{old_name}'의 새 아티스트명을 입력하세요: ").strip()

                    if not new_name:
                        print("❌ 새 아티스트명이 입력되지 않았습니다. 취소합니다.")
                        continue

                    if new_name in artists:
                        print(f"❌ 아티스트 '{new_name}'은(는) 이미 존재합니다. 다른 이름을 사용해주세요.")
                        continue

                    # DataFrame 업데이트
                    df.loc[choice_idx, 'artist'] = new_name

                    # CSV 파일 저장
                    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                    print(f"✅ 아티스트명이 '{old_name}'에서 '{new_name}'(으)로 성공적으로 변경되었습니다.")
                    # Break the loop after a successful update to show the updated list
                    break

                except ValueError:
                    print("❌ 숫자를 입력하거나 'q'를 입력하여 종료하세요.")
                except Exception as e:
                    print(f"❌ 업데이트 중 오류 발생: {e}")

        except Exception as e:
            print(f"❌ 파일 처리 중 오류 발생: {e}")

    def edit_song_title(self):
        """songs.csv에서 노래 제목을 수정합니다."""
        csv_path = os.path.join(self.output_dir, 'songs.csv')
        if not os.path.exists(csv_path):
            print(f"❌ {csv_path} 파일을 찾을 수 없습니다.")
            return

        try:
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            df['title'] = df['title'].fillna('')
            df['artist'] = df['artist'].fillna('')
        except FileNotFoundError:
            print(f"❌ songs.csv 파일을 찾을 수 없습니다: {csv_path}")
            return
        except Exception as e:
            print(f"❌ 파일 처리 중 오류 발생: {e}")
            return

        artist_name = input("\n곡을 찾을 아티스트명을 입력하세요: ").strip()
        if not artist_name:
            print("❌ 아티스트명이 입력되지 않았습니다.")
            return

        artist_songs_mask = df['artist'].str.contains(artist_name, case=False, na=False, regex=False)
        if not artist_songs_mask.any():
            print(f"❌ '{artist_name}' 아티스트의 곡을 찾을 수 없습니다.")
            return

        # 백업 생성
        backup_dir = os.path.join(self.output_dir, 'backups', datetime.now().strftime('%Y%m%d_%H%M%S'))
        os.makedirs(backup_dir, exist_ok=True)
        backup_path = os.path.join(backup_dir, 'songs.csv')
        try:
            shutil.copy2(csv_path, backup_path)
            print(f"📋 원본 파일 백업 완료: {backup_path}")
        except Exception as e:
            print(f"⚠️ 백업 실패: {e}. 작업을 중단합니다.")
            return

        while True:
            # 루프마다 최신 DataFrame 상태에서 다시 필터링
            artist_songs = df[df['artist'].str.contains(artist_name, case=False, na=False, regex=False)].copy()
            artist_songs.reset_index(inplace=True)

            if artist_songs.empty:
                print("더 이상 수정할 곡이 없습니다.")
                break

            print("\n" + "-"*60)
            print(f"🎤 '{artist_name}'의 노래 목록")
            print("-"*60)
            for i, row in artist_songs.iterrows():
                print(f"{i+1}. {row['title']}")

            print("\n'q'를 입력하여 종료")
            choice = input("수정할 곡 번호를 선택하세요: ").strip()

            if choice.lower() == 'q':
                break

            try:
                choice_idx = int(choice) - 1
                if not (0 <= choice_idx < len(artist_songs)):
                    print("❌ 잘못된 번호입니다.")
                    continue

                original_song_row = artist_songs.loc[choice_idx]
                original_title = original_song_row['title']
                original_df_index = original_song_row['index']

                new_title = input(f"'{original_title}'의 새 제목을 입력하세요: ").strip()

                if not new_title:
                    print("❌ 새 제목이 입력되지 않았습니다. 취소합니다.")
                    continue

                # DataFrame 업데이트
                df.loc[original_df_index, 'title'] = new_title

                # 변경된 내용을 CSV 파일에 즉시 저장
                df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                print(f"✅ 곡 제목이 성공적으로 '{new_title}' (으)로 변경되었습니다.")

            except ValueError:
                print("❌ 숫자를 입력하거나 'q'를 입력하여 종료하세요.")
            except Exception as e:
                print(f"❌ 업데이트 중 오류 발생: {e}")
                break

    def add_new_artists(self):
        """artists.csv에 신규 아티스트를 추가합니다."""
        csv_path = os.path.join(self.output_dir, 'artists.csv')
        if not os.path.exists(csv_path):
            print("❌ artists.csv 파일을 찾을 수 없습니다.")
            return

        try:
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            existing_artists = set(df['artist'].dropna().str.strip())

            print("\n" + "-"*60)
            print("🎤 신규 아티스트 추가 (여러 명은 쉼표(,)로 구분)")
            print("-"*60)
            new_artists_input = input("추가할 아티스트명을 입력하세요: ").strip()

            if not new_artists_input:
                print("❌ 입력된 아티스트가 없습니다.")
                return

            new_artists = [name.strip() for name in new_artists_input.split(',') if name.strip()]
            added_artists = []
            new_rows = []

            for artist_name in new_artists:
                if artist_name in existing_artists:
                    print(f"ℹ️ 아티스트 '{artist_name}'은(는) 이미 존재합니다. 건너뜁니다.")
                else:
                    new_row = {
                        'id': '',
                        'artist': artist_name,
                        'debut_date': '',
                        'nationality': '',
                        'group_type': '',
                        'introduction': '',
                        'social_media': '',
                        'keywords': '',
                        'img_url': '',
                        'created_at': '',
                        'updated_at': ''
                    }
                    new_rows.append(new_row)
                    added_artists.append(artist_name)
                    existing_artists.add(artist_name) # Add to set to prevent duplicate additions in the same run

            if new_rows:
                new_df = pd.DataFrame(new_rows)
                df = pd.concat([df, new_df], ignore_index=True)
                df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                print(f"\n✅ 다음 아티스트가 성공적으로 추가되었습니다: {', '.join(added_artists)}")
            else:
                print("\nℹ️ 추가할 새로운 아티스트가 없습니다.")

        except Exception as e:
            print(f"❌ 아티스트 추가 중 오류 발생: {e}")

    def fill_missing_artists(self):
        """concerts.csv에서 아티스트가 비어있는 콘서트에 아티스트명을 채워넣습니다."""
        csv_path = os.path.join(self.output_dir, 'concerts.csv')
        if not os.path.exists(csv_path):
            print("❌ concerts.csv 파일을 찾을 수 없습니다.")
            return

        try:
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            # NaN 값을 빈 문자열로 대체하고 양쪽 공백 제거
            df['artist'] = df['artist'].fillna('').str.strip()
            
            missing_artist_concerts = df[df['artist'] == ''].copy()

            if missing_artist_concerts.empty:
                print("✅ 아티스트 정보가 비어있는 콘서트가 없습니다.")
                return

            while True:
                print("\n" + "-"*60)
                print("🎤 아티스트가 비어있는 콘서트 목록")
                print("-"*60)
                
                # 목록 다시 로드 및 출력
                df = pd.read_csv(csv_path, encoding='utf-8-sig')
                df['artist'] = df['artist'].fillna('').str.strip()
                missing_artist_concerts = df[df['artist'] == ''].copy()

                if missing_artist_concerts.empty:
                    print("✅ 모든 콘서트에 아티스트 정보가 채워졌습니다.")
                    break

                for i, (index, row) in enumerate(missing_artist_concerts.iterrows()):
                    print(f"{i+1}. {row['title']}")
                
                print("\n'q'를 입력하여 종료")
                choice = input("아티스트를 추가할 콘서트 번호를 선택하세요: ").strip()

                if choice.lower() == 'q':
                    break
                
                try:
                    choice_idx = int(choice) - 1
                    if not (0 <= choice_idx < len(missing_artist_concerts)):
                        print("❌ 잘못된 번호입니다.")
                        continue
                    
                    # 선택된 콘서트의 실제 DataFrame 인덱스
                    concert_index = missing_artist_concerts.index[choice_idx]
                    concert_title = missing_artist_concerts.loc[concert_index, 'title']
                    
                    new_artist = input(f"'{concert_title}'의 아티스트명을 입력하세요: ").strip()
                    
                    if new_artist:
                        # DataFrame 업데이트
                        df.loc[concert_index, 'artist'] = new_artist
                        
                        # CSV 파일 저장
                        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                        print(f"✅ '{concert_title}'의 아티스트가 '{new_artist}'(으)로 업데이트되었습니다.")
                    else:
                        print("❌ 아티스트명이 입력되지 않았습니다. 취소합니다.")

                except ValueError:
                    print("❌ 숫자를 입력하거나 'q'를 입력하여 종료하세요.")
                except Exception as e:
                    print(f"❌ 업데이트 중 오류 발생: {e}")
        except Exception as e:
            print(f"❌ 파일 처리 중 오류 발생: {e}")

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
        mask = df['artist'].str.contains(keyword, case=False, na=False, regex=False)
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


    def add_lyrics_to_songs(self):
        """songs.csv의 아티스트를 선택하여 가사를 추가합니다."""
        csv_path = os.path.join(self.output_dir, 'songs.csv')
        if not os.path.exists(csv_path):
            print(f"❌ {csv_path} 파일을 찾을 수 없습니다.")
            return

        try:
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            df['artist'] = df['artist'].fillna('')
            df['lyrics'] = df['lyrics'].fillna('')
            
            # 고유 아티스트 목록 추출
            unique_artists = df['artist'].unique()
            unique_artists = [a for a in unique_artists if a.strip()]
            
            # 가사 없는 곡이 있는 아티스트만 필터링
            artists_with_missing_lyrics = []
            for artist in unique_artists:
                artist_songs = df[df['artist'] == artist]
                songs_without_lyrics = len(artist_songs[artist_songs['lyrics'].str.strip() == ''])
                if songs_without_lyrics > 0:
                    artists_with_missing_lyrics.append({
                        'name': artist,
                        'total': len(artist_songs),
                        'missing': songs_without_lyrics
                    })
            
            if not artists_with_missing_lyrics:
                print("✅ 모든 아티스트의 곡에 가사가 있습니다.")
                return
            
            # 가사 없는 곡 수 기준 내림차순 정렬
            artists_with_missing_lyrics.sort(key=lambda x: x['missing'], reverse=True)
            
            print("\n" + "-"*60)
            print("🎤 가사가 없는 곡이 있는 아티스트 목록")
            print("-"*60)
            
            for i, artist_info in enumerate(artists_with_missing_lyrics, 1):
                print(f"{i}. {artist_info['name']} (전체: {artist_info['total']}곡, 가사 없음: {artist_info['missing']}곡)")
            
            print("\n'q'를 입력하여 종료")
            print("단일 선택: 번호 (예: 1)")
            print("복수 선택: 쉼표 구분 (예: 1,3,5) 또는 범위 (예: 1-3)")
            print("전체 선택: all")
            choice = input("아티스트 번호를 선택하세요: ").strip()

            if choice.lower() == 'q':
                return

            try:
                # 선택 파싱
                selected_indices = []
                if choice.lower() == 'all':
                    selected_indices = list(range(len(artists_with_missing_lyrics)))
                elif '-' in choice and ',' not in choice:
                    parts = choice.split('-')
                    if len(parts) == 2:
                        start, end = int(parts[0]) - 1, int(parts[1]) - 1
                        if 0 <= start <= end < len(artists_with_missing_lyrics):
                            selected_indices = list(range(start, end + 1))
                        else:
                            print("❌ 잘못된 범위입니다.")
                            return
                    else:
                        print("❌ 잘못된 범위 형식입니다.")
                        return
                else:
                    for token in choice.split(','):
                        idx = int(token.strip()) - 1
                        if not (0 <= idx < len(artists_with_missing_lyrics)):
                            print(f"❌ 잘못된 번호: {token.strip()}")
                            return
                        if idx not in selected_indices:
                            selected_indices.append(idx)

                selected_artists = [artists_with_missing_lyrics[i] for i in selected_indices]
                script_path = Path(__file__).parent.parent / 'lyrics' / 'artist_lyrics_update.py'

                # 단일 선택이면 검색용 아티스트명 입력 받기
                if len(selected_artists) == 1:
                    selected_artist = selected_artists[0]['name']
                    print(f"\n선택된 아티스트: {selected_artist}")
                    search_artist = input("검색용 아티스트명 (Enter시 자동 추출): ").strip()

                    if search_artist:
                        command = [sys.executable, str(script_path), csv_path, selected_artist, search_artist]
                    else:
                        command = [sys.executable, str(script_path), csv_path, selected_artist]

                    print(f"\n🚀 가사 업데이트 시작: {selected_artist}")
                    print("-"*60)
                    subprocess.run(command)
                    print(f"\n✅ '{selected_artist}' 가사 업데이트 완료!")

                else:
                    # 복수 선택이면 순차 처리 (검색용 아티스트명은 자동 추출)
                    print(f"\n선택된 아티스트 {len(selected_artists)}명:")
                    for i, info in enumerate(selected_artists, 1):
                        print(f"  {i}. {info['name']} (가사 없음: {info['missing']}곡)")
                    print("\n복수 선택 시 검색용 아티스트명은 자동 추출됩니다.")
                    confirm = input("진행하시겠습니까? (y/n): ").strip().lower()
                    if confirm != 'y':
                        print("취소했습니다.")
                        return

                    success_count = 0
                    for i, artist_info in enumerate(selected_artists, 1):
                        artist_name = artist_info['name']
                        print(f"\n[{i}/{len(selected_artists)}] 🚀 가사 업데이트 시작: {artist_name}")
                        print("-"*60)
                        command = [sys.executable, str(script_path), csv_path, artist_name]
                        result = subprocess.run(command)
                        if result.returncode == 0:
                            print(f"✅ '{artist_name}' 가사 업데이트 완료!")
                            success_count += 1
                        else:
                            print(f"⚠️ '{artist_name}' 가사 업데이트 중 오류 발생")

                    print(f"\n{'='*60}")
                    print(f"전체 완료: {success_count}/{len(selected_artists)}명 성공")

            except ValueError:
                print("❌ 숫자를 입력해주세요.")
            except FileNotFoundError:
                print(f"❌ 스크립트 파일을 찾을 수 없습니다: {script_path}")
            except Exception as e:
                print(f"❌ 가사 업데이트 중 오류 발생: {e}")
                
        except Exception as e:
            print(f"❌ 파일 처리 중 오류 발생: {e}")
        
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

    @staticmethod
    def _fix_artist_name(name: str) -> str:
        """아티스트명을 '영문 (한국어)' 형식으로 교정"""
        if not name or '(' not in name or ')' not in name:
            return name
        normalized = re.sub(r'\s*\(', ' (', name).strip()
        before = normalized[:normalized.index('(')].strip()
        inside = normalized[normalized.index('(') + 1:normalized.index(')')].strip()
        if any('가' <= c <= '힣' for c in before):
            return f"{inside} ({before})"
        return normalized

    def fix_artist_name_format(self):
        """DB의 아티스트명 형식을 '영문 (한국어)'로 일괄 교정"""
        print("\n" + "="*60)
        print("아티스트명 형식 일괄 교정")
        print("="*60)
        print("어느 DB를 사용할까요?")
        print("  1. Dev DB")
        print("  2. 프로덕션 DB")
        print("  3. Stage DB")
        db_choice = input("선택 (1-3): ").strip()

        if db_choice == '1':
            db = get_dev_db_manager()
        elif db_choice == '2':
            db = get_db_manager()
        elif db_choice == '3':
            db = get_stage_db_manager()
        else:
            print("❌ 잘못된 선택입니다.")
            return

        if not db.connect_with_ssh():
            print("❌ DB 연결 실패")
            return

        try:
            db.cursor.execute("SELECT id, artist FROM artists")
            rows = db.cursor.fetchall()

            targets = []
            for artist_id, artist in rows:
                fixed = self._fix_artist_name(artist)
                if fixed != artist:
                    targets.append((artist_id, artist, fixed))

            if not targets:
                print("수정할 아티스트명이 없습니다.")
                return

            print(f"\n수정 대상 {len(targets)}개:\n")
            for artist_id, before, after in targets:
                print(f"  [{artist_id}] {before!r} → {after!r}")

            confirm = input("\n위 내용으로 수정하시겠습니까? (yes/no): ").strip().lower()
            if confirm != 'yes':
                print("취소됨.")
                return

            for artist_id, before, after in targets:
                db.cursor.execute("UPDATE artists SET artist = %s WHERE id = %s", (after, artist_id))
                db.cursor.execute("UPDATE concerts SET artist = %s WHERE artist = %s", (after, before))

            db.commit()
            print(f"\n{len(targets)}개 아티스트명 수정 완료.")

        except Exception as e:
            db.rollback()
            print(f"❌ 오류: {e}")
        finally:
            db.disconnect()

    def close(self):
        """연결 종료"""
        self.connection = None
        self.cursor = None
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
                    self.interactive_fix_individual_fields()
                elif choice == '4':
                    self.interactive_search()
                elif choice == '5':
                    self.delete_data_menu()
                elif choice == '6':
                    self.fix_artist_name_format()
                elif choice == '7':
                    print("프로그램을 종료합니다.")
                    break
                else:
                    print("❌ 잘못된 선택입니다. 1-7 중에서 선택해주세요.")
                
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