## About Project

![](https://github.com/user-attachments/assets/c53dd5d8-d984-45b4-9993-cf635859a5ff)

> Perplexity API를 활용해 콘서트 데이터 수집 시스템을 구현했습니다.

## 설치 방법

```bash
git clone https://github.com/mediunn/Livith-Data
cd Livith-Data
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 환경 설정

`.env` 파일에 API 키들을 설정해주세요.

### Gemini 2.0 Flash API 사용 (권장)
Google AI Studio의 Gemini 2.0 Flash with Google Search grounding을 사용합니다:
```
GEMINI_API_KEY=your_gemini_api_key_here
KOPIS_API_KEY=your_kopis_api_key_here
USE_GEMINI_API=true           # 기본값
GEMINI_USE_SEARCH=true        # Google Search grounding 사용 (실시간 검색)
GEMINI_MODEL_VERSION=2.0      # Gemini 2.0 사용
```

**🚀 Gemini 2.0의 주요 기능:**
- **Google Search grounding**: 실시간 웹 검색으로 최신 정보 수집
- **URL context**: 특정 URL을 참조하여 정보 추출
- **향상된 정확도**: 더 정확하고 최신의 콘서트 정보 제공

[Gemini API 키 받기](https://aistudio.google.com/app/apikey)

### Perplexity API 사용 (대체 옵션)
Perplexity API를 사용하려면:
```
PERPLEXITY_API_KEY=your_perplexity_api_key_here
KOPIS_API_KEY=your_kopis_api_key_here
USE_GEMINI_API=false
```

## 사용 방법

### 전체 실행
모든 단계를 순차적으로 실행합니다:
```bash
python3 src/main.py
```

### 단계별 실행
프로젝트는 5개 단계로 구성되어 있으며, 각 단계를 독립적으로 실행할 수 있습니다:

#### 단계 설명
1. **단계 1**: KOPIS API에서 공연 데이터 수집 및 필터링
2. **단계 2**: 기본 콘서트 정보 수집 (Perplexity API)
3. **단계 3**: 상세 데이터 수집 (아티스트, 셋리스트, 곡, 문화)
4. **단계 4**: 굿즈(MD) 정보 수집
5. **단계 5**: 아티스트명 매칭 및 정리

#### 실행 옵션
```bash
# 특정 단계만 실행
python3 src/main.py --stage 1    # 단계 1만 실행 (KOPIS 데이터 수집)
python3 src/main.py --stage 2    # 단계 2만 실행 (기본 정보 수집)
python3 src/main.py --stage 3    # 단계 3만 실행 (상세 정보 수집)
python3 src/main.py --stage 4    # 단계 4만 실행 (굿즈 정보 수집)
python3 src/main.py --stage 5    # 단계 5만 실행 (아티스트명 매칭)

# 범위 지정 실행
python3 src/main.py --from 2     # 단계 2부터 끝까지 실행
python3 src/main.py --from 3 --to 4  # 단계 3과 4만 실행

# 개별 스크립트 실행
python3 src/stage1_fetch_kopis.py    # 단계 1 독립 실행
python3 src/stage2_collect_basic.py   # 단계 2 독립 실행
python3 src/stage3_collect_detailed.py # 단계 3 독립 실행
python3 src/stage4_collect_merchandise.py # 단계 4 독립 실행
python3 src/stage5_match_artists.py   # 단계 5 독립 실행
```

각 단계는 이전 단계의 결과를 CSV 파일에서 읽어오므로 독립적으로 재실행 가능합니다.

## 프로젝트 구조

```
📁 Wable-iOS
├── 📁 src: 소스 코드
├── 📁 output: 생성된 CSV 파일들
├── 📁 tests: 테스트 코드
```
