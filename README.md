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

`.env` 파일에 Perplexity API 키를 설정해주세요.

```
PERPLEXITY_API_KEY=your_actual_api_key_here
```

## 사용 방법

프로젝트 경로에서 터미널을 열고 아래 명령어를 입력해주세요.

```bash
python3 src/main.py
```

## 프로젝트 구조

```
📁 Wable-iOS
├── 📁 src: 소스 코드
├── 📁 output: 생성된 CSV 파일들
├── 📁 tests: 테스트 코드
```
