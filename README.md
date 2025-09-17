## 📊 About Project

![project](https://github.com/user-attachments/assets/c53dd5d8-d984-45b4-9993-cf635859a5ff)

> 한국 내한 콘서트 데이터 수집 및 관리 시스템

## 📁 프로젝트 구조

```
Livith-Data/
├── setup/              # 설치 및 설정 파일
│   ├── install.sh      # macOS/Linux 설치 스크립트
│   ├── install.bat     # Windows 설치 스크립트
│   ├── activate.sh     # 가상환경 활성화 스크립트
│   ├── requirements.txt # Python 의존성
│   └── pyproject.toml  # 프로젝트 설정
├── scripts/            # 개발 도구 및 설정
│   ├── .claude/        # Claude Code 설정
│   └── .github/        # GitHub 설정 (이슈, PR 템플릿)
├── core/               # 핵심 시스템
│   ├── pipeline/       # 메인 데이터 수집 파이프라인
│   └── apis/          # API 모듈 (Gemini, KOPIS 등)
├── tools/             # 사용자 도구
│   ├── data/          # 데이터 관리 도구
│   ├── database/      # 데이터베이스 관리
│   └── lyrics/        # 가사 관련 도구
├── lib/               # 공통 라이브러리
├── data/              # 데이터 파일
├── logs/              # 로그 파일
├── .env               # 환경 변수 설정
├── .gitignore         # Git 제외 파일
└── README.md          # 프로젝트 문서
```

## 🚀 빠른 시작

### 자동 설치 (권장)

#### Windows
```cmd
# 저장소 클론
git clone https://github.com/mediunn/livith-data.git
cd livith-data

# 자동 설치 실행
setup\install.bat
```

#### macOS/Linux
```bash
# 저장소 클론
git clone https://github.com/mediunn/livith-data.git
cd livith-data

# 자동 설치 실행
chmod +x setup/install.sh
setup/install.sh
```

### 수동 설치

#### 1. 필수 요구사항
- **Python 3.8 이상**
- **pip** (Python 패키지 관리자)
- **Git** (선택사항, SSH 기능용)

#### 2. 의존성 설치
```bash
# 가상환경 생성
python -m venv venv

# 가상환경 활성화
# Windows:
venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate

# 패키지 설치
pip install -r setup/requirements.txt
```

#### 3. 환경 설정
```bash
# .env 파일 생성
cp .env.example .env

# .env 파일에서 API 키와 데이터베이스 정보 설정
```

### 플랫폼별 특이사항

#### Windows 사용자
- **OpenSSH 설치 필요**: SSH 터널 기능 사용 시
- **PowerShell 권장**: CMD보다 호환성이 좋음
- **경로 설정**: SSH 키는 `C:\Users\YourName\.ssh\` 경로 사용

#### macOS 사용자  
- **Homebrew 권장**: Python 설치에 사용
- **Xcode Command Line Tools**: Git 사용을 위해 필요

#### Linux 사용자
- **패키지 관리자**: apt, yum, dnf 등으로 Python3 설치
- **SSH 클라이언트**: 대부분 기본 설치됨

## 💻 컨벤션

## Prefix (Tag)

<div align="center">

<table>
  <thead>
    <tr>
      <th>Prefix</th>
      <th>설명</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>Feat</code></td>
      <td>기능 구현</td>
    </tr>
    <tr>
      <td><code>Add</code></td>
      <td>파일(이미지, 폰트 등 포함) 추가</td>
    </tr>
    <tr>
      <td><code>Delete</code></td>
      <td>파일 삭제</td>
    </tr>
    <tr>
      <td><code>Chore</code></td>
      <td>이외 자잘한 수정</td>
    </tr>
    <tr>
      <td><code>Refactor</code></td>
      <td>코드의 비즈니스 로직 수정</td>
    </tr>
    <tr>
      <td><code>Fix</code></td>
      <td>버그 등의 기능 전체 수정</td>
    </tr>
    <tr>
      <td><code>Setting</code></td>
      <td>프로젝트 설정</td>
    </tr>
    <tr>
      <td><code>Docs</code></td>
      <td>문서 작성</td>
    </tr>
  </tbody>
</table>

</div>

## Message

> [Prefix] #이슈번호 - 메세지 내용  
> 

```markdown
[Feat] #1 - 로그인 기능 구현
```

</div>

## 🚀 주요 실행 명령어

### 1. 메인 데이터 수집 파이프라인
```bash
# 전체 파이프라인 실행 (1-5단계)
python core/pipeline/main.py

# 특정 스테이지만 실행
python core/pipeline/main.py --stage 3

# 테스트 모드
python core/pipeline/main.py --test

# 전체 재수집 모드
python core/pipeline/main.py --full
```

### 2. 데이터베이스 관리
```bash
# CSV → MySQL 업로드 (전체 테이블)
python tools/database/upsert_csv_to_mysql.py

# MySQL → CSV 다운로드 (전체 테이블)
python tools/database/download_mysql_to_csv.py
```

### 3. 데이터 수정 및 관리
```bash
# 데이터 수정 도구
python tools/data/fix_data.py

# 가사 업데이트
python tools/lyrics/update_lyrics.py

# 가사 번역
python tools/lyrics/translate_lyrics.py

# songs 테이블만 업데이트
python tools/data/update_songs_only.py
```

## 📊 메인 사용 스크립트

### 🎯 핵심 파이프라인
| 스크립트 | 기능 설명 | 사용법 |
|---------|----------|--------|
| `core/pipeline/main.py` | **메인 실행기** - 5단계 데이터 수집 파이프라인 | `python core/pipeline/main.py [옵션]` |

### 🔧 데이터베이스 관리
| 스크립트 | 기능 설명 | 사용법 |
|---------|----------|--------|
| `tools/database/upsert_csv_to_mysql.py` | **CSV → MySQL 업로드** (전체 테이블 자동) | `python tools/database/upsert_csv_to_mysql.py` |
| `tools/database/download_mysql_to_csv.py` | **MySQL → CSV 다운로드** (전체 테이블 자동) | `python tools/database/download_mysql_to_csv.py` |

### 🛠 데이터 관리 도구
| 스크립트 | 기능 설명 | 사용법 |
|---------|----------|--------|
| `tools/data/fix_data.py` | **데이터 수정 도구** | `python tools/data/fix_data.py` |
| `tools/lyrics/update_lyrics.py` | **가사 자동 업데이트** | `python tools/lyrics/update_lyrics.py` |
| `tools/lyrics/translate_lyrics.py` | **가사 번역 및 발음 변환** | `python tools/lyrics/translate_lyrics.py` |
| `tools/data/update_songs_only.py` | **songs 테이블만 업데이트** | `python tools/data/update_songs_only.py` |

<details>
<summary>🔍 <strong>고급 도구 (클릭하여 보기)</strong></summary>

### 개별 스테이지 실행
| 스크립트 | 기능 설명 | 사용법 |
|---------|----------|--------|
| `core/pipeline/stage1_fetch_kopis.py` | KOPIS API 데이터 수집 | `python core/pipeline/stage1_fetch_kopis.py` |
| `core/pipeline/stage2_collect_basic.py` | 기본 정보 수집 | `python core/pipeline/stage2_collect_basic.py` |
| `core/pipeline/stage3_collect_detailed.py` | 상세 정보 수집 | `python core/pipeline/stage3_collect_detailed.py` |
| `core/pipeline/stage4_collect_merchandise.py` | MD 정보 수집 | `python core/pipeline/stage4_collect_merchandise.py` |
| `core/pipeline/stage5_match_artists.py` | 아티스트 매칭 | `python core/pipeline/stage5_match_artists.py` |

### 기타 유틸리티
| 스크립트 | 기능 설명 | 사용법 |
|---------|----------|--------|
| `core/pipeline/update_concert_status.py` | 콘서트 상태 업데이트 | `python core/pipeline/update_concert_status.py` |
| `tools/lyrics/artist_lyrics_update.py` | 아티스트별 가사 업데이트 | `python tools/lyrics/artist_lyrics_update.py` |
| `tools/lyrics/manual_lyrics_update.py` | 수동 가사 업데이트 | `python tools/lyrics/manual_lyrics_update.py` |
| `tools/data/update_concerts_sorting.py` | 콘서트 정렬 업데이트 | `python tools/data/update_concerts_sorting.py` |
| `tools/data/fix_concerts_data.py` | 콘서트 데이터 수정 | `python tools/data/fix_concerts_data.py` |
| `tools/data/merge_songs_to_setlist.py` | songs → setlist 병합 | `python tools/data/merge_songs_to_setlist.py` |

### 데이터베이스 분석/관리
| 스크립트 | 기능 설명 | 사용법 |
|---------|----------|--------|
| `tools/database/check_db_schema.py` | DB 스키마 확인 | `python tools/database/check_db_schema.py` |
| `tools/database/analyze_table_constraints.py` | 테이블 제약조건 분석 | `python tools/database/analyze_table_constraints.py` |
| `tools/data/check_connection_info.py` | 연결 정보 확인 | `python tools/data/check_connection_info.py` |

</details>

## 🗂 시스템 구성요소

### 핵심 모듈 (자동 호출)
- `core/pipeline/stages.py` - 스테이지 구현체
- `lib/enhanced_data_collector.py` - AI 기반 데이터 수집기
- `core/apis/gemini_api.py`, `core/apis/perplexity_api.py` - AI API 통합
- `core/apis/kopis_api.py` - KOPIS API 통합
- `lib/` - 공통 유틸리티 (config, prompts, safe_writer)
- `tools/database/ssh_mysql_connection.py` - DB 연결 관리

## 💡 주요 작업별 가이드

### 1. 새로운 콘서트 데이터 수집
```bash
python core/pipeline/main.py
```

### 2. 데이터 수정이 필요한 경우
```bash
python tools/data/fix_data.py
```

### 3. 데이터베이스 동기화
```bash
# 로컬 CSV → 서버 MySQL 업로드 (자동으로 모든 테이블)
python tools/database/upsert_csv_to_mysql.py

# 서버 MySQL → 로컬 CSV 다운로드 (자동으로 모든 테이블)
python tools/database/download_mysql_to_csv.py
```

### 4. 가사 관련 작업
```bash
# 가사 수집
python tools/lyrics/update_lyrics.py

# 가사 번역
python tools/lyrics/translate_lyrics.py
```

## 🔑 환경 설정

### 기본 설정
`.env` 파일에 필요한 API 키와 데이터베이스 정보를 설정하세요:

#### 필수 API 키

```env
# API Keys
KOPIS_API_KEY=your_key_here
GEMINI_API_KEY=your_key_here
PERPLEXITY_API_KEY=your_key_here
MUSIXMATCH_API_KEY=your_key_here

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

### 플랫폼별 일반적인 문제

#### Windows
- **ModuleNotFoundError**: 가상환경이 활성화되지 않음 → `activate.bat` 실행
- **SSH 연결 오류**: OpenSSH 미설치 → Windows 기능에서 OpenSSH 클라이언트 설치
- **권한 오류**: PowerShell을 관리자 권한으로 실행

#### macOS
- **Command not found: python**: Python3 설치 필요 → `brew install python3`
- **SSL 인증서 오류**: 인증서 업데이트 → `/Applications/Python\ 3.x/Install\ Certificates.command` 실행

#### Linux
- **python3-venv 패키지 없음**: 가상환경 패키지 설치 → `sudo apt install python3-venv`
- **MySQL 연결 오류**: 개발 라이브러리 설치 → `sudo apt install libmysqlclient-dev`

### 일반적인 오류

#### SSH 터널 오류
- **Windows**: `os.setsid()` 관련 → `lib/platform_utils.py`에서 자동 처리됨
- **연결 실패**: SSH 키 권한 확인 → `chmod 400 ~/.ssh/your-key.pem`

#### 데이터 인코딩 문제  
- MySQL 연결 시 `charset='utf8mb4'` 설정 확인
- CSV 저장 시 `encoding='utf-8-sig'` 사용

#### API 관련 오류
- **Rate Limit**: 요청 간격 조정 → `.env`에서 `REQUEST_DELAY` 증가
- **API 키 오류**: `.env` 파일의 키 값 확인
