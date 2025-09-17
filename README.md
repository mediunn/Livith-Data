# Livith Data Collection System

한국 내한 콘서트 데이터 수집 및 관리 시스템

## 📁 프로젝트 구조

```
Livith-Data/
├── src/                # 메인 파이프라인 코드
├── scripts/            # 유틸리티 스크립트
├── database/           # 데이터베이스 관리
├── data_processing/    # 데이터 처리 모듈
├── utils/              # 공통 유틸리티
├── output/             # 출력 CSV 파일
└── logs/               # 로그 파일
```

## 🚀 메인 실행 명령어

```bash
# 전체 파이프라인 실행
python src/main.py

# 특정 스테이지만 실행
python src/main.py --stage 3

# 테스트 모드 (제한된 데이터)
python src/main.py --test

# 전체 재수집 모드
python src/main.py --full
```

## 📊 사용 가능한 스크립트 목록

### 🎯 Core Pipeline (src/)

| 스크립트 | 기능 설명 | 사용법 |
|---------|----------|--------|
| `main.py` | 메인 파이프라인 실행 | `python src/main.py [옵션]` |
| `stages.py` | 5단계 데이터 수집 프로세스 | main.py에서 자동 호출 |
| `stage1_fetch_kopis.py` | KOPIS API 데이터 수집 | stages.py에서 호출 |
| `stage2_collect_basic.py` | 기본 정보 수집 (AI API) | stages.py에서 호출 |
| `stage3_collect_detailed.py` | 상세 정보 수집 | stages.py에서 호출 |
| `stage4_collect_merchandise.py` | MD 정보 수집 | stages.py에서 호출 |
| `stage5_match_artists.py` | 아티스트 매칭 | stages.py에서 호출 |
| `update_concert_status.py` | 콘서트 상태 업데이트 | `python src/update_concert_status.py` |
| `artist_matcher.py` | 아티스트 이름 매칭 유틸리티 | 내부 모듈 |
| `artist_name_mapper.py` | 아티스트 이름 매핑 | 내부 모듈 |
| `gemini_api.py` | Gemini AI API 통합 | 내부 모듈 |
| `kopis_api.py` | KOPIS API 통합 | 내부 모듈 |
| `perplexity_api.py` | Perplexity API 통합 | 내부 모듈 |
| `lyrics_translator.py` | 가사 번역 모듈 | 내부 모듈 |
| `lyrics_updater.py` | 가사 업데이트 | 내부 모듈 |
| `musixmatch_lyrics_api.py` | Musixmatch API 통합 | 내부 모듈 |

### 🔧 Database Management (database/)

| 스크립트 | 기능 설명 | 사용법 |
|---------|----------|--------|
| `ssh_mysql_connection.py` | SSH 터널 + MySQL 연결 | 모듈로 import 사용 |
| `mysql_data_loader.py` | MySQL 데이터 로더 | 모듈로 import 사용 |
| `upsert_csv_to_mysql.py` | CSV → MySQL 업로드 (전체) | `python database/upsert_csv_to_mysql.py` |
| `selective_upsert_csv_to_mysql.py` | 선택적 CSV 업로드 | `python database/selective_upsert_csv_to_mysql.py` |
| `final_improved_upsert_csv_to_mysql.py` | 개선된 CSV 업로드 | `python database/final_improved_upsert_csv_to_mysql.py` |
| `download_mysql_to_csv.py` | MySQL → CSV 다운로드 | `python database/download_mysql_to_csv.py` |
| `csv_to_mysql_loader.py` | CSV 로더 유틸리티 | 내부 모듈 |
| `check_db_schema.py` | DB 스키마 확인 | `python database/check_db_schema.py` |
| `analyze_table_constraints.py` | 테이블 제약조건 분석 | `python database/analyze_table_constraints.py` |
| `add_concert_schedules.py` | 콘서트 일정 추가 | `python database/add_concert_schedules.py` |
| `update_concert_setlists.py` | 세트리스트 업데이트 | `python database/update_concert_setlists.py` |
| `fix_concert_setlists.py` | 세트리스트 수정 | `python database/fix_concert_setlists.py` |
| `upload_remaining_tables.py` | 남은 테이블 업로드 | `python database/upload_remaining_tables.py` |
| `remove_md_duplicates.py` | MD 중복 제거 | `python database/remove_md_duplicates.py` |
| `remove_schedule_duplicates.py` | 일정 중복 제거 | `python database/remove_schedule_duplicates.py` |
| `remove_section_duplicates.py` | 섹션 중복 제거 | `python database/remove_section_duplicates.py` |
| `simple_ssh_mysql.py` | 간단한 SSH MySQL 연결 | `python database/simple_ssh_mysql.py` |

### 🛠 Data Processing Scripts (scripts/)

| 스크립트 | 기능 설명 | 사용법 |
|---------|----------|--------|
| `fix_data.py` | 대화형 데이터 수정 도구 | `python scripts/fix_data.py` |
| `update_songs_only.py` | songs 테이블만 업데이트 | `python scripts/update_songs_only.py` |
| `update_concerts_sorting.py` | 콘서트 정렬 업데이트 | `python scripts/update_concerts_sorting.py` |
| `update_lyrics.py` | 가사 업데이트 | `python scripts/update_lyrics.py` |
| `artist_lyrics_update.py` | 아티스트별 가사 업데이트 | `python scripts/artist_lyrics_update.py` |
| `manual_lyrics_update.py` | 수동 가사 업데이트 | `python scripts/manual_lyrics_update.py` |
| `translate_lyrics.py` | 가사 번역 | `python scripts/translate_lyrics.py` |
| `clean_lyrics_format.py` | 가사 형식 정리 | `python scripts/clean_lyrics_format.py` |
| `add_translation_to_songs.py` | 번역 추가 | `python scripts/add_translation_to_songs.py` |
| `clean_songs_translation.py` | 번역 데이터 정리 | `python scripts/clean_songs_translation.py` |
| `final_clean_songs_translation.py` | 최종 번역 정리 | `python scripts/final_clean_songs_translation.py` |
| `clear_translation_data.py` | 번역 데이터 초기화 | `python scripts/clear_translation_data.py` |
| `convert_pronunciation_to_translation.py` | 발음 → 번역 변환 | `python scripts/convert_pronunciation_to_translation.py` |
| `fix_natural_korean.py` | 자연스러운 한국어 수정 | `python scripts/fix_natural_korean.py` |
| `fix_concerts_data.py` | 콘서트 데이터 수정 | `python scripts/fix_concerts_data.py` |
| `create_proper_setlists.py` | 올바른 세트리스트 생성 | `python scripts/create_proper_setlists.py` |
| `fix_setlist_songs_structure.py` | 세트리스트 구조 수정 | `python scripts/fix_setlist_songs_structure.py` |
| `fill_empty_setlists.py` | 빈 세트리스트 채우기 | `python scripts/fill_empty_setlists.py` |
| `merge_songs_to_setlist.py` | songs → setlist 병합 | `python scripts/merge_songs_to_setlist.py` |
| `sync_songs_setlist.py` | songs-setlist 동기화 | `python scripts/sync_songs_setlist.py` |
| `split_multi_day_concerts.py` | 멀티데이 콘서트 분할 | `python scripts/split_multi_day_concerts.py` |
| `convert_setlist_songs_to_mysql_format.py` | MySQL 형식 변환 | `python scripts/convert_setlist_songs_to_mysql_format.py` |
| `check_connection_info.py` | 연결 정보 확인 | `python scripts/check_connection_info.py` |
| `csv_to_sql.py` | CSV → SQL 변환 | `python scripts/csv_to_sql.py` |
| `upload_to_mysql.py` | MySQL 업로드 | `python scripts/upload_to_mysql.py` |
| `upload_schedule_only.py` | 일정만 업로드 | `python scripts/upload_schedule_only.py` |
| `upload_setlist_songs_only.py` | 세트리스트곡만 업로드 | `python scripts/upload_setlist_songs_only.py` |
| `upload_setlists_and_setlist_songs.py` | 세트리스트+곡 업로드 | `python scripts/upload_setlists_and_setlist_songs.py` |
| `upload_songs_and_setlist_songs.py` | songs+세트리스트곡 업로드 | `python scripts/upload_songs_and_setlist_songs.py` |

### 📦 Data Processing Modules (data_processing/)

| 스크립트 | 기능 설명 | 사용법 |
|---------|----------|--------|
| `enhanced_data_collector.py` | AI 기반 데이터 수집기 | 모듈로 import 사용 |
| `enhanced_csv_manager.py` | CSV 관리 유틸리티 | 모듈로 import 사용 |
| `data_models.py` | 데이터 모델 정의 | 모듈로 import 사용 |

### 🔨 Utilities (utils/)

| 스크립트 | 기능 설명 | 사용법 |
|---------|----------|--------|
| `config.py` | 환경 설정 관리 | 모듈로 import 사용 |
| `prompts.py` | AI 프롬프트 템플릿 | 모듈로 import 사용 |
| `safe_writer.py` | 안전한 파일 쓰기 | 모듈로 import 사용 |

## 💡 주요 작업별 사용 가이드

### 1. 새로운 콘서트 데이터 수집
```bash
# 최신 콘서트 데이터 수집 (Stage 1-5 전체)
python src/main.py

# 특정 기간 데이터 재수집
python src/main.py --full
```

### 2. 데이터베이스 업로드
```bash
# 전체 CSV 데이터 업로드
python database/upsert_csv_to_mysql.py

# 특정 테이블만 업로드
python database/selective_upsert_csv_to_mysql.py
```

### 3. 데이터 수정 및 정리
```bash
# 대화형 데이터 수정
python scripts/fix_data.py

# 가사 업데이트
python scripts/update_lyrics.py

# 세트리스트 정리
python scripts/create_proper_setlists.py
```

### 4. 데이터베이스 백업
```bash
# MySQL → CSV 백업
python database/download_mysql_to_csv.py
```

## 🔑 환경 설정

`.env` 파일에 필요한 API 키와 데이터베이스 정보를 설정하세요:

```env
# API Keys
KOPIS_API_KEY=your_key_here
GEMINI_API_KEY=your_key_here
PERPLEXITY_API_KEY=your_key_here

# Database
DB_HOST=your_host
DB_USER=your_user
DB_PASSWORD=your_password
DB_NAME=your_database

# SSH (for remote DB)
SSH_HOST=your_ssh_host
SSH_USER=your_ssh_user
SSH_KEY_PATH=path/to/key
```

## 📝 주의사항

- 모든 스크립트는 프로젝트 루트 디렉토리에서 실행
- CSV 파일은 UTF-8 인코딩 사용
- 데이터베이스 작업 전 백업 권장
- API 호출 시 Rate Limit 주의

## 🐛 문제 해결

SSH 터널 오류 발생 시:
- Windows: `os.setsid()` 관련 오류는 플랫폼 호환성 문제
- 해결: `database/ssh_mysql_connection.py` 수정 필요

데이터 인코딩 문제:
- MySQL 연결 시 `charset='utf8mb4'` 설정 확인
- CSV 저장 시 `encoding='utf-8-sig'` 사용