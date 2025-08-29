# Livith-Data 📊

## About Project

![](https://github.com/user-attachments/assets/c53dd5d8-d984-45b4-9993-cf635859a5ff)

> AI 기반 콘서트 데이터 수집 시스템 - Gemini 2.0 Flash with Google Search grounding 활용

## 🚀 설치 방법

```bash
git clone https://github.com/mediunn/Livith-Data
cd Livith-Data
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 🔧 환경 설정

`.env` 파일에 API 키들을 설정해주세요:

```env
GEMINI_API_KEY=your_gemini_api_key_here
KOPIS_API_KEY=your_kopis_api_key_here
USE_GEMINI_API=true           # Gemini 사용 (기본값)
GEMINI_USE_SEARCH=true        # Google Search grounding 사용
GEMINI_MODEL_VERSION=2.0      # Gemini 2.0 사용
```

**🌟 Gemini 2.0 Flash 주요 기능:**
- **실시간 웹 검색**: Google Search grounding으로 최신 정보 수집
- **향상된 정확도**: 더 정확한 콘서트/아티스트 정보 제공
- **URL 컨텍스트**: 특정 웹페이지 참조 가능

## 📁 프로젝트 구조

```
📦 Livith-Data/
├── 📂 src/                    # 🏗️ 메인 소스코드
│   ├── main.py               # 전체 파이프라인 실행
│   ├── stages.py             # 통합 스테이지 실행기
│   ├── stage1_fetch_kopis.py      # 1단계: KOPIS 데이터 수집
│   ├── stage2_collect_basic.py    # 2단계: 기본 콘서트 정보 수집
│   ├── stage3_collect_detailed.py # 3단계: 상세 데이터 수집
│   ├── stage4_collect_merchandise.py # 4단계: 굿즈 정보 수집
│   ├── stage5_match_artists.py    # 5단계: 아티스트명 매칭
│   ├── kopis_api.py          # KOPIS API 연동
│   ├── gemini_api.py         # Gemini API 연동
│   ├── perplexity_api.py     # Perplexity API 연동
│   ├── artist_name_mapper.py # 아티스트명 매핑 유틸리티
│   ├── artist_matcher.py     # 아티스트 매칭 로직
│   ├── update_concert_status.py # 콘서트 상태 업데이트
│   └── 📂 deprecated/        # 사용 중단된 파일들
│
├── 📂 data_processing/        # 🔄 데이터 처리 모듈
│   ├── data_models.py        # 데이터 모델 정의
│   ├── enhanced_data_collector.py # 고도화된 데이터 수집기
│   └── enhanced_csv_manager.py    # CSV 파일 관리
│
├── 📂 utils/                 # 🛠️ 유틸리티
│   ├── config.py            # 설정 관리
│   ├── prompts.py           # AI 프롬프트 관리
│   └── safe_writer.py       # 안전한 파일 쓰기
│
├── 📂 scripts/               # 🔧 유틸리티 스크립트
│   ├── fix_data.py          # ⭐ 데이터 수정 도구 (메인)
│   ├── update_concerts_sorting.py # 콘서트 정렬 업데이트
│   ├── fix_concerts_data.py # 콘서트 데이터 수정
│   ├── update_lyrics.py     # 가사 정보 업데이트
│   ├── check_connection_info.py # MySQL 연결 테스트
│   └── 📂 deprecated/        # 분석용/임시 스크립트들
│
├── 📂 database/              # 💾 데이터베이스 관련
│   ├── upsert_csv_to_mysql.py # CSV → MySQL 업로드
│   ├── simple_ssh_mysql.py  # SSH MySQL 연결
│   ├── mysql_data_loader.py # MySQL 데이터 로더
│   └── ssh_mysql_connection.py # SSH MySQL 연결 유틸
│
├── 📂 output/                # 📊 결과 파일
│   ├── 📂 main_output/      # 메인 결과물
│   ├── 📂 test_output/      # 테스트 결과물
│   └── 📂 backups/          # 백업 파일
│
├── 📂 test/                  # 🧪 테스트 코드
│   └── 📂 deprecated/        # 사용 중단된 테스트들
│
├── 📂 logs/                  # 📝 로그 파일
├── 📂 docs/                  # 📚 문서
└── 📂 backup/                # 💾 수동 백업 파일들
```

## 🎯 사용 가능한 커맨드

### 1. 🏗️ 메인 데이터 수집

#### 전체 파이프라인 실행
```bash
# 모든 단계 순차 실행
python3 src/main.py

# 또는 통합 스테이지 실행기 사용
python3 -m src.stages
```

#### 단계별 실행
```bash
# 특정 단계만 실행
python3 src/main.py --stage 1    # KOPIS 데이터 수집
python3 src/main.py --stage 2    # 기본 정보 수집
python3 src/main.py --stage 3    # 상세 정보 수집
python3 src/main.py --stage 4    # 굿즈 정보 수집
python3 src/main.py --stage 5    # 아티스트 매칭

# 범위 지정 실행
python3 src/main.py --from 2 --to 4  # 2~4단계만 실행
python3 src/main.py --from 3         # 3단계부터 끝까지
```

#### 개별 스크립트 실행
```bash
python3 src/stage1_fetch_kopis.py     # KOPIS API 데이터 수집
python3 src/stage2_collect_basic.py   # 기본 콘서트 정보 수집
python3 src/stage3_collect_detailed.py # 상세 데이터 수집
python3 src/stage4_collect_merchandise.py # 굿즈 정보 수집
python3 src/stage5_match_artists.py   # 아티스트명 매칭
```

### 2. 🔧 데이터 수정 도구

#### 데이터 수정 및 관리
```bash
# 대화형 데이터 수정 도구
python3 scripts/fix_data.py --interactive

# 아티스트명/콘서트명 검색
python3 scripts/fix_data.py --search "JVKE" --type artist
python3 scripts/fix_data.py --search "콘서트명" --type concert

# 테스트 모드 (안전한 테스트)
OUTPUT_MODE=test python3 scripts/fix_data.py --interactive
```

**🎯 데이터 수정 도구 기능:**
- 🔍 **검색**: 아티스트명/콘서트명 부분 매칭 검색
- 🔄 **CSV 수정**: 로컬 CSV 파일들 일괄 업데이트 (자동 백업)
- 💾 **MySQL 반영**: 데이터베이스 직접 업데이트
- 🔗 **연관 수정**: 여러 CSV 파일의 관련 필드 동시 수정

### 3. 💾 데이터베이스 관리

#### MySQL 데이터 업로드
```bash
# CSV 파일들을 MySQL에 업로드
python3 database/upsert_csv_to_mysql.py

# 콘서트 정렬 상태 업데이트
python3 scripts/update_concerts_sorting.py

# 콘서트 데이터 수정
python3 scripts/fix_concerts_data.py
```

### 4. 🧪 테스트 및 검증

#### 환경 모드 설정
```bash
# 테스트 모드 (output/test_output 사용)
export OUTPUT_MODE=test

# 프로덕션 모드 (output/main_output 사용)
export OUTPUT_MODE=production
# 또는
unset OUTPUT_MODE
```

## 📊 데이터 수집 단계

### 🏗️ 데이터 수집 파이프라인

| 단계 | 설명 | 입력 | 출력 | 소요시간 |
|------|------|------|------|----------|
| **1단계** | KOPIS API 데이터 수집 | KOPIS API | `kopis_filtered_concerts.csv` | ~2분 |
| **2단계** | 기본 콘서트 정보 수집 | 1단계 결과 | `step1_basic_concerts.csv` | ~5분 |
| **3단계** | 상세 데이터 수집 | 2단계 결과 | 모든 CSV 파일 | ~10분 |
| **4단계** | 굿즈 정보 수집 | 3단계 결과 | `md.csv` 업데이트 | ~3분 |
| **5단계** | 아티스트명 매칭 | 4단계 결과 | 최종 정리 | ~2분 |

### 📈 생성되는 데이터 파일

| 파일명 | 설명 | 주요 컬럼 |
|--------|------|----------|
| `concerts.csv` | 콘서트 기본 정보 | artist, title, start_date, status, label, introduction |
| `artists.csv` | 아티스트 정보 | artist, birth_date, debut_date, nationality, group_type |
| `setlists.csv` | 셋리스트 정보 | artist_name, concert_title, type, song_count |
| `songs.csv` | 곡 정보 | title, artist, album, release_year |
| `cultures.csv` | 팬 문화 정보 | artist_name, concert_title, title, content |
| `schedule.csv` | 일정 정보 | concert_title, category, scheduled_at |
| `md.csv` | 굿즈 정보 | artist_name, concert_title, item_name, price |
| `concert_info.csv` | 콘서트 부가 정보 | artist_name, concert_title, category, content |

## ⚙️ 설정 옵션

### 환경 변수
```bash
# API 선택
USE_GEMINI_API=true          # Gemini API 사용 (기본값)
USE_GEMINI_API=false         # Perplexity API 사용

# 출력 모드
OUTPUT_MODE=test             # 테스트 출력 (/output/test_output)
OUTPUT_MODE=production       # 프로덕션 출력 (/output/main_output)

# Gemini 설정
GEMINI_USE_SEARCH=true       # Google Search grounding 사용
GEMINI_MODEL_VERSION=2.0     # Gemini 2.0 Flash 사용
```

### API 키 설정
- **KOPIS API**: [KOPIS 개발자 센터](https://www.kopis.or.kr/por/cs/openapi/openApiList.do)
- **Gemini API**: [Google AI Studio](https://aistudio.google.com/app/apikey)
- **Perplexity API**: [Perplexity API](https://docs.perplexity.ai/)

## 🔍 데이터 품질 관리

### 자동 백업
- 모든 데이터 수정 시 자동 백업 생성
- 백업 위치: `output/backups/` + 타임스탬프

### 데이터 검증
- 중복 데이터 자동 제거
- 필수 필드 유효성 검사
- KOPIS 데이터 보존 우선

### 오류 처리
- API 호출 실패 시 fallback 데이터 제공
- 로그 파일을 통한 오류 추적
- 단계별 독립 실행으로 장애 격리

### 코드 정리
- **deprecated 폴더**: 사용 중단된 파일들 보관
  - `src/deprecated/`: 이전 버전 소스코드
  - `scripts/deprecated/`: 분석용/임시 스크립트들  
  - `test/deprecated/`: 사용 중단된 테스트 코드들
- **활성 코드만 메인 디렉토리 유지**: 깔끔한 프로젝트 구조

## 🆘 문제 해결

### 자주 발생하는 문제

**1. API 키 오류**
```bash
# .env 파일 확인
cat .env

# API 키 유효성 테스트
python3 -c "from src.gemini_api import GeminiAPI; api = GeminiAPI(); print('API 연결 성공')"
```

**2. 데이터 수정 필요**
```bash
# 잘못된 아티스트명 수정
python3 scripts/fix_data.py --search "잘못된이름" --type artist
python3 scripts/fix_data.py --interactive
```

**3. MySQL 연결 문제**
```bash
# SSH 터널 상태 확인
ps aux | grep ssh

# MySQL 연결 테스트
python3 scripts/check_connection_info.py
```

## 📝 로그 및 디버깅

### 로그 파일 위치
- `logs/mysql_data_load.log` - 데이터베이스 로드 로그
- `logs/lyrics_update.log` - 가사 업데이트 로그
- `logs/safe_lyrics_update.log` - 안전한 가사 업데이트 로그

### 디버그 모드
```bash
# 상세 로그 출력
python3 src/main.py --verbose

# 특정 단계 디버깅
python3 src/stage3_collect_detailed.py --debug
```

---

> ⚡ **빠른 시작**: `python3 src/main.py`로 전체 파이프라인을 실행하거나, `python3 scripts/fix_data.py --interactive`로 데이터를 수정하세요!