# 🎵 가사 업데이트 도구 설정 및 사용법

Musixmatch API를 사용하여 `songs.csv` 파일들의 가사 정보를 자동으로 업데이트하는 도구입니다.

## ⚙️ 설정

### 1. 환경변수 설정
```bash
# .env 파일 생성
touch .env

# .env 파일을 열어 API 키 설정
nano .env
```

### 2. Musixmatch API 키 발급
1. [Musixmatch Developer Portal](https://developer.musixmatch.com/) 접속
2. 계정 가입 또는 로그인
3. 새 애플리케이션 생성 후 API 키 발급
4. `.env` 파일에 다음과 같이 설정:
   ```
   MUSIXMATCH_API_KEY=your_musixmatch_api_key_here
   ```

## 🚀 사용법

### 1. 기본 사용 (모든 곡 처리)
```bash
python3 update_lyrics.py
```

### 2. 테스트 모드 (파일당 3곡만)
```bash
python3 update_lyrics.py --test
```

### 3. 제한 모드 (파일당 N곡만)
```bash
python3 update_lyrics.py --max-songs 5
```

## 📁 처리 대상

- `output/` 디렉토리 하위의 모든 `songs.csv` 파일
- 각 CSV 파일의 `lyrics` 컬럼이 비어있는 곡들

## 🎯 동작 방식

1. **환경변수 확인**: `.env` 파일에서 `MUSIXMATCH_API_KEY` 로드
2. **파일 검색**: `output/` 디렉토리에서 `songs.csv` 파일들을 자동 검색
3. **가사 검색**: 각 곡의 `title`과 `artist` 정보로 Musixmatch에서 검색
4. **가사 업데이트**: 찾은 가사를 `lyrics` 컬럼에 저장, `musixmatch_url` 추가
5. **백업**: 원본 파일은 `.backup` 확장자로 백업
6. **로그**: 상세한 처리 로그를 `lyrics_update.log`에 저장

## ✨ 주요 기능

- ✅ **환경변수 관리**: `.env` 파일을 통한 안전한 API 키 관리
- ✅ **자동 백업**: 원본 파일 보호
- ✅ **중복 방지**: 이미 가사가 있는 곡은 스킵
- ✅ **가사 전문 제공**: Musixmatch에서 실제 가사 텍스트 제공
- ✅ **오류 처리**: API 오류나 네트워크 문제 자동 처리
- ✅ **진행률 표시**: 실시간 처리 상황 확인
- ✅ **상세 로그**: 모든 처리 과정 기록
- ✅ **URL 저장**: Musixmatch 페이지 링크 자동 생성

## 📊 결과 예시

```
🎵 가사 업데이트 완료 결과
============================================================
📁 처리된 파일: 3개
🎼 전체 곡 수: 45곡
✅ 업데이트됨: 38곡
⏭️  스킵됨: 5곡 (이미 가사 있음)
❌ 실패: 2곡
============================================================

📊 파일별 상세 결과:
  📄 song.csv
    - 총 15곡, 업데이트 12곡, 스킵 2곡, 실패 1곡
```

## ⚠️ 주의사항

- **환경변수**: `.env` 파일에 `MUSIXMATCH_API_KEY` 반드시 설정
- **API 제한**: 무료 계정은 하루 2,000회 요청 제한, 요청 간 1.5초 지연
- **저작권**: 일부 곡은 저작권으로 인해 가사 제공 안됨
- **네트워크**: 안정적인 인터넷 연결 필요
- **백업**: 처리 전 자동으로 원본 파일 백업됨

## 🔧 파일 구조

```
├── .env                           # 환경변수 파일 (생성 필요)
├── config.py                      # 설정 관리
├── update_lyrics.py               # 메인 실행 스크립트
├── lyrics_update.log              # 처리 로그 (자동 생성)
└── src/
    ├── musixmatch_lyrics_api.py   # Musixmatch API 인터페이스
    └── lyrics_updater.py          # CSV 파일 업데이트 로직
```

## 🐛 문제 해결

- **"MUSIXMATCH_API_KEY 환경변수가 설정되지 않았습니다"**: `.env` 파일 확인
- **"songs.csv 파일이 없습니다"**: `output/` 디렉토리에 CSV 파일이 있는지 확인
- **API 오류**: 네트워크 연결 및 API 키 유효성 확인
- **가사 찾기 실패**: 곡명/아티스트명 정확성 확인
- **요청 한도 초과**: 24시간 후 재시도 또는 유료 계정 업그레이드

## 📝 로그 확인

처리 과정의 상세한 정보는 `lyrics_update.log` 파일에서 확인할 수 있습니다.