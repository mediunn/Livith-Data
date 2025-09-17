# 🎵 Livith-Data 프로젝트 스크립트 사용 가이드

## 📁 디렉토리 구조
- **루트**: `/Users/youz2me/Xcode/Livith-Data/`
- **스크립트**: `/Users/youz2me/Xcode/Livith-Data/scripts/` (21개)
- **데이터베이스**: `/Users/youz2me/Xcode/Livith-Data/database/` (26개)

## 🚀 주요 스크립트 사용법

### 1. 🌟 번역 관련 스크립트

#### 📝 AI 번역 생성
```bash
# Higher Power 같은 원어 가사를 한국어로 번역
python scripts/add_translation_to_songs.py
```
- **필요**: `GEMINI_API_KEY` 환경변수 설정
- **기능**: 가사 있는 곡에 발음+해석 자동 생성
- **입력**: `output/main_output/songs.csv`
- **출력**: 번역 추가된 CSV + 백업파일

#### 🧹 번역 데이터 정리
```bash
# 번역 필드 형식 정리
python scripts/clean_songs_translation.py

# 최종 번역 정리 (콘서트 정보 제거 등)
python scripts/final_clean_songs_translation.py
```

#### 🧪 번역 테스트
```bash
# 단일 곡 번역 테스트
python scripts/test_ai_translation.py

# 번역 형식 수동 테스트
python scripts/manual_test_translation.py
```

### 2. 🗄️ 데이터베이스 관리

#### ⬆️ 데이터 업로드
```bash
# 전체 CSV를 MySQL로 업로드
python database/upsert_csv_to_mysql.py

# 선택적 CSV 업로드 (파일별 개별 업로드)
python database/selective_upsert_csv_to_mysql.py

# 중복 방지 개선된 업로드
python database/final_improved_upsert_csv_to_mysql.py

# songs 테이블만 업데이트
python scripts/update_songs_only.py
```
- **선택적 업로드**: 대화형으로 원하는 테이블만 선택해서 업로드 가능

#### ⬇️ 데이터 다운로드
```bash
# MySQL 전체 테이블을 CSV로 다운로드
python database/download_mysql_to_csv.py
```

#### 🔍 데이터베이스 확인
```bash
# DB 스키마 확인
python database/check_db_schema.py

# 연결 상태 테스트
python scripts/check_connection_info.py
```

### 3. 🎭 콘서트 정보 관리

#### 📝 콘서트 정보 텍스트 수정
```bash
# AI를 사용한 잘린 텍스트 수정 (100자 제한)
python scripts/fix_concert_info_truncation.py

# AI 없이 수동으로 텍스트 수정 (100자 제한)
python scripts/fix_concert_info_manually.py
```
- **기능**: concert_info.csv의 잘린 텍스트를 자연스럽게 완성
- **제한**: 모든 content는 100자 이내로 제한
- **입력**: `output/main_output/concert_info.csv`
- **출력**: 수정된 CSV + 자동 백업

### 4. 🛠️ 데이터 정리 및 수정

#### 🇰🇷 한국어 문체 수정
```bash
# CSV 파일의 부자연스러운 한국어 종결어미와 문장 구성 수정
python scripts/fix_natural_korean.py
```
- **기능**: 내용 변경 없이 종결어미와 문장 구성만 자연스럽게 수정
- **처리 파일**: concert_info.csv, concerts.csv, songs.csv, cultures.csv
- **수정 내용**: 어색한 종결어미, 이중 존댓말, 맞춤법, 중복 표현 등

#### 🎯 종합 데이터 관리 (추천)
```bash
# 대화형 데이터 수정 도구
python scripts/fix_data.py --interactive

# 특정 아티스트 검색
python scripts/fix_data.py --search "JVKE" --type artist
```

#### 📝 가사 관리
```bash
# 가사 형식 정리
python scripts/clean_lyrics_format.py output/main_output/songs.csv

# 특정 아티스트 가사 업데이트
python scripts/artist_lyrics_update.py output/main_output/songs.csv "Pink Sweat$ (핑크스웨츠)"
```

#### 🔄 중복 제거
```bash
# 각 테이블별 중복 제거
python database/remove_md_duplicates.py
python database/remove_schedule_duplicates.py
python database/remove_section_duplicates.py
```

## ⚙️ 환경 설정

### 📋 필수 패키지 설치
```bash
pip install pandas mysql-connector-python google-generativeai python-dotenv sshtunnel
```

### 🔑 환경변수 설정
```bash
# .env 파일에 추가하거나 export로 설정
export GEMINI_API_KEY="your_gemini_api_key_here"
```

### 🗝️ SSH 키 파일
- **위치**: `/Users/youz2me/Downloads/livith-key.pem`
- **용도**: AWS RDS MySQL 연결용 SSH 터널
- **권한**: `chmod 600 /Users/youz2me/Downloads/livith-key.pem`

## 💡 사용 시나리오별 가이드

### 🎵 새 곡 번역 작업
```bash
# 1. AI로 번역 생성
python scripts/add_translation_to_songs.py

# 2. 번역 정리
python scripts/clean_songs_translation.py
python scripts/final_clean_songs_translation.py

# 3. DB 업로드
python scripts/update_songs_only.py
```

### 🔄 전체 데이터 동기화
```bash
# 1. DB에서 최신 데이터 다운로드
python database/download_mysql_to_csv.py

# 2. 데이터 정리 및 수정
python scripts/fix_data.py --interactive

# 3. 정리된 데이터 업로드
python database/final_improved_upsert_csv_to_mysql.py
```

### 🧹 데이터 품질 개선
```bash
# 1. 가사 형식 정리
python scripts/clean_lyrics_format.py output/main_output/songs.csv

# 2. 중복 제거
python database/remove_md_duplicates.py
python database/remove_schedule_duplicates.py

# 3. 데이터 검증 및 수정
python scripts/fix_data.py --interactive
```

## 📊 주요 파일 위치

### 📁 입력 파일
- **메인 데이터**: `output/main_output/songs.csv`
- **정리된 데이터**: `output/cleaned_data/songs.csv`
- **SSH 키**: `/Users/youz2me/Downloads/livith-key.pem`

### 📁 출력 파일
- **백업**: 자동생성 (타임스탬프 포함)
- **로그**: 스크립트별 실시간 출력

## ⚠️ 주의사항

### 🔒 보안
- SSH 키 파일 권한 확인: `chmod 600 livith-key.pem`
- GEMINI_API_KEY는 환경변수로만 설정

### 💾 백업
- 모든 스크립트는 자동 백업 생성
- 중요한 작업 전에는 수동 백업 권장

### 🌐 네트워크
- SSH 터널 연결 필요 (AWS RDS 접근용)
- API 요청 제한 고려 (Gemini AI)

## 🆘 트러블슈팅

### 연결 문제
```bash
# SSH 터널 확인
ssh -i /Users/youz2me/Downloads/livith-key.pem ubuntu@43.203.48.65

# MySQL 연결 테스트
python scripts/check_connection_info.py
```

### API 문제
```bash
# GEMINI API 키 확인
echo $GEMINI_API_KEY

# 번역 테스트
python scripts/test_ai_translation.py
```

### 데이터 문제
```bash
# 스키마 확인
python database/check_db_schema.py

# 대화형 수정 도구
python scripts/fix_data.py --interactive
```

---

## 🎯 빠른 참조

| 작업 | 명령어 |
|------|--------|
| **번역 생성** | `python scripts/add_translation_to_songs.py` |
| **콘서트 정보 수정** | `python scripts/fix_concert_info_manually.py` |
| **한국어 문체 수정** | `python scripts/fix_natural_korean.py` |
| **전체 업로드** | `python database/upsert_csv_to_mysql.py` |
| **선택적 업로드** | `python database/selective_upsert_csv_to_mysql.py` |
| **데이터 다운로드** | `python database/download_mysql_to_csv.py` |
| **대화형 수정** | `python scripts/fix_data.py --interactive` |
| **가사 정리** | `python scripts/clean_lyrics_format.py songs.csv` |
| **스키마 확인** | `python database/check_db_schema.py` |

이 가이드를 통해 Livith-Data 프로젝트의 모든 스크립트를 효율적으로 활용하세요! 🚀