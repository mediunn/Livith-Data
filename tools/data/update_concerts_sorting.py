#!/usr/bin/env python3
import pandas as pd
from datetime import datetime, date
import subprocess
import mysql.connector
from mysql.connector import Error
import time
import signal
import os
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))
from lib.config import Config

class ConcertsSortingUpdater:
    def __init__(self):
        self.ssh_process = None
        self.connection = None
        self.cursor = None
        self.csv_path = str(Config.OUTPUT_DIR / 'concerts.csv')

    def analyze_current_data(self):
        """현재 CSV 데이터 분석"""
        try:
            print("=" * 80)
            print("📊 현재 concerts.csv 분석")
            print("=" * 80)
            
            # CSV 읽기
            df = pd.read_csv(self.csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"• 총 레코드: {len(df)}개")
            print(f"• 컬럼: {list(df.columns)}")
            
            # 현재 sorted_index 분석
            print(f"\n🔢 현재 sorted_index 분석:")
            sorted_index_counts = df['sorted_index'].value_counts().sort_index()
            print(f"  • 고유값 개수: {len(sorted_index_counts)}")
            print(f"  • 값 분포: {dict(sorted_index_counts)}")
            
            # 날짜 분석 
            print(f"\n📅 날짜 분석:")
            today = date.today()
            print(f"  • 오늘 날짜: {today}")
            
            # start_date 기준으로 분류
            date_categories = {'past': 0, 'today': 0, 'future': 0, 'invalid': 0}
            
            for _, row in df.iterrows():
                try:
                    start_date = datetime.strptime(row['start_date'], '%Y-%m-%d').date()
                    if start_date < today:
                        date_categories['past'] += 1
                    elif start_date == today:
                        date_categories['today'] += 1
                    else:
                        date_categories['future'] += 1
                except:
                    date_categories['invalid'] += 1
            
            print(f"  • 날짜 분류:")
            for category, count in date_categories.items():
                print(f"    - {category}: {count}개")
            
            # 현재 status 분포
            print(f"\n📊 현재 status 분포:")
            status_counts = df['status'].value_counts()
            for status, count in status_counts.items():
                print(f"  • {status}: {count}개")
            
            return df, today
            
        except Exception as e:
            print(f"❌ 분석 실패: {e}")
            return None, None

    def update_csv_data(self, df, today):
        """CSV 데이터 업데이트 (sorted_index와 status)"""
        try:
            print(f"\n" + "=" * 80)
            print("🔧 CSV 데이터 업데이트")
            print("=" * 80)
            
            # 백업 생성
            backup_path = self.csv_path + f".backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            df.to_csv(backup_path, index=False, encoding='utf-8')
            print(f"✅ 백업 생성: {backup_path}")
            
            # 날짜 기준으로 정렬
            # 1. 과거 이벤트 (start_date 오름차순)
            # 2. 오늘/미래 이벤트 (start_date 오름차순)
            
            updated_df = df.copy()
            updated_records = []
            
            for i, (_, row) in enumerate(df.iterrows()):
                try:
                    start_date = datetime.strptime(row['start_date'], '%Y-%m-%d').date()
                    end_date = datetime.strptime(row['end_date'], '%Y-%m-%d').date() if row['end_date'] else start_date
                    
                    # Status 결정
                    if end_date < today:
                        new_status = 'COMPLETED'
                    elif start_date <= today <= end_date:
                        new_status = 'ONGOING'
                    else:
                        new_status = 'UPCOMING'
                    
                    # 레코드 업데이트
                    updated_record = row.copy()
                    updated_record['status'] = new_status
                    updated_record['date_for_sorting'] = start_date  # 정렬용 임시 컬럼
                    updated_record['row_index'] = i  # 원래 순서 보존용
                    
                    updated_records.append(updated_record)
                    
                except Exception as e:
                    print(f"⚠️ 날짜 파싱 실패 (row {i}): {e}")
                    # 날짜 파싱 실패시 원본 유지
                    updated_record = row.copy()
                    updated_record['status'] = 'ONGOING'
                    updated_record['date_for_sorting'] = today
                    updated_record['row_index'] = i
                    updated_records.append(updated_record)
            
            # DataFrame으로 변환
            updated_df = pd.DataFrame(updated_records)
            
            # 정렬 로직:
            # 1. COMPLETED (과거) - start_date 내림차순 (최근 완료된 것 먼저)  
            # 2. ONGOING (진행 중) - start_date 오름차순
            # 3. UPCOMING (예정) - start_date 오름차순 (가까운 것 먼저)
            
            completed_df = updated_df[updated_df['status'] == 'COMPLETED'].sort_values('date_for_sorting', ascending=False)
            ongoing_df = updated_df[updated_df['status'] == 'ONGOING'].sort_values('date_for_sorting', ascending=True)
            upcoming_df = updated_df[updated_df['status'] == 'UPCOMING'].sort_values('date_for_sorting', ascending=True)
            
            # 순서대로 합치기
            final_df = pd.concat([completed_df, ongoing_df, upcoming_df], ignore_index=True)
            
            # sorted_index 재할당 (0부터 시작)
            final_df['sorted_index'] = range(len(final_df))
            
            # 임시 컬럼 제거
            final_df = final_df.drop(['date_for_sorting', 'row_index'], axis=1)
            
            # 결과 확인
            print(f"\n📊 업데이트 결과:")
            new_status_counts = final_df['status'].value_counts()
            for status, count in new_status_counts.items():
                print(f"  • {status}: {count}개")
            
            print(f"\n🔢 새로운 sorted_index:")
            print(f"  • COMPLETED: 0 ~ {len(completed_df)-1}")
            print(f"  • ONGOING: {len(completed_df)} ~ {len(completed_df)+len(ongoing_df)-1}")
            print(f"  • UPCOMING: {len(completed_df)+len(ongoing_df)} ~ {len(final_df)-1}")
            
            # 샘플 확인
            print(f"\n📋 정렬 결과 샘플:")
            for i, (_, row) in enumerate(final_df.head(10).iterrows()):
                status_emoji = {'COMPLETED': '✅', 'ONGOING': '🔄', 'UPCOMING': '⏳'}.get(row['status'], '❓')
                print(f"  {row['sorted_index']:2d}. {status_emoji} {row['start_date']} | {row['title'][:40]}...")
            
            # CSV 저장
            final_df.to_csv(self.csv_path, index=False, encoding='utf-8')
            print(f"\n✅ 업데이트된 CSV 저장 완료: {self.csv_path}")
            
            return final_df
            
        except Exception as e:
            print(f"❌ CSV 업데이트 실패: {e}")
            return None

    def create_ssh_tunnel(self):
        """SSH 터널 생성"""
        try:
            print(f"\n🔧 SSH 터널 생성 중...")
            
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

    def update_database(self, updated_df):
        """데이터베이스 업데이트"""
        try:
            print(f"\n" + "=" * 80)
            print("💾 데이터베이스 업데이트")
            print("=" * 80)
            
            # 기존 concerts 데이터 확인
            print("📈 기존 concerts 데이터 확인...")
            # 삭제하지 않고 업서트 모드로 진행  
            # self.cursor.execute("DELETE FROM concerts")
            # self.connection.commit()
            
            # artist_id 매핑
            self.cursor.execute("SELECT id, artist FROM artists")
            artist_mapping = {artist: id for id, artist in self.cursor.fetchall()}
            
            # 삽입 쿼리
            insert_query = """
                INSERT INTO concerts (
                    title, artist, artist_id, start_date, end_date, 
                    status, poster, code, sorted_index, ticket_site, 
                    ticket_url, venue, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            current_time = datetime.now()
            data_to_insert = []
            
            for _, row in updated_df.iterrows():
                artist_name = row['artist']
                artist_id = artist_mapping.get(artist_name)
                
                if artist_id:
                    data_to_insert.append((
                        row['title'],
                        artist_name,
                        artist_id,
                        row['start_date'],
                        row['end_date'],
                        row['status'],
                        row.get('poster', ''),
                        row.get('code', ''),
                        int(row['sorted_index']),
                        row.get('ticket_site', ''),
                        row.get('ticket_url', ''),
                        row.get('venue', ''),
                        current_time,
                        current_time
                    ))
            
            print(f"📝 데이터 삽입 중... ({len(data_to_insert)}개)")
            self.cursor.executemany(insert_query, data_to_insert)
            self.connection.commit()
            
            print(f"✅ concerts 테이블에 {len(data_to_insert)}개 삽입 완료")
            
            # 결과 확인
            self.verify_database_update()
            
            return True
            
        except Exception as e:
            print(f"❌ 데이터베이스 업데이트 실패: {e}")
            self.connection.rollback()
            return False

    def verify_database_update(self):
        """데이터베이스 업데이트 결과 확인"""
        try:
            print(f"\n📊 데이터베이스 업서트 확인:")
            
            # 전체 레코드 수
            self.cursor.execute("SELECT COUNT(*) FROM concerts")
            total_count = self.cursor.fetchone()[0]
            print(f"  • 총 레코드 수: {total_count}개")
            
            # status 분포
            self.cursor.execute("SELECT status, COUNT(*) FROM concerts GROUP BY status ORDER BY status")
            status_counts = self.cursor.fetchall()
            print(f"  • status 분포:")
            for status, count in status_counts:
                print(f"    - {status}: {count}개")
            
            # sorted_index 순으로 샘플 확인
            print(f"\n📋 정렬 순서 확인 (처음 10개):")
            self.cursor.execute("""
                SELECT sorted_index, status, start_date, title 
                FROM concerts 
                ORDER BY sorted_index 
                LIMIT 10
            """)
            samples = self.cursor.fetchall()
            
            for sorted_idx, status, start_date, title in samples:
                status_emoji = {'COMPLETED': '✅', 'ONGOING': '🔄', 'UPCOMING': '⏳'}.get(status, '❓')
                print(f"  {sorted_idx:2d}. {status_emoji} {start_date} | {title[:45]}...")
                
        except Exception as e:
            print(f"❌ 검증 실패: {e}")

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

def main():
    """메인 실행"""
    updater = ConcertsSortingUpdater()
    
    try:
        # 1. 현재 데이터 분석
        df, today = updater.analyze_current_data()
        if df is None:
            return
        
        # 2. CSV 업데이트
        updated_df = updater.update_csv_data(df, today)
        if updated_df is None:
            return
        
        # 3. 데이터베이스 업데이트
        if not updater.create_ssh_tunnel():
            return
        
        if not updater.connect_mysql():
            return
        
        updater.update_database(updated_df)
        
        print(f"\n" + "=" * 80)
        print("🎉 concerts 데이터 정렬 및 상태 업데이트 완료!")
        print("=" * 80)
        
    except KeyboardInterrupt:
        print("\n⏹️ 사용자 중단")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
    finally:
        updater.close()

if __name__ == "__main__":
    main()