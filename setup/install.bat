@echo off
REM Livith Data 설치 스크립트

echo 🚀 Livith Data 설치 시작...

REM Python 확인
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python 필요
    pause
    exit /b 1
)

REM 가상환경 생성
echo 🔧 가상환경 생성...
if exist venv rmdir /s /q venv
python -m venv venv

REM 의존성 설치
echo 📦 패키지 설치...
call venv\Scripts\activate
pip install --upgrade pip
pip install -r setup\requirements.txt
pip install sshtunnel

REM 환경 파일 복사
if not exist .env (
    if exist .env.example (
        copy .env.example .env >nul
        echo ✅ .env 파일 생성
    )
)

REM 활성화 스크립트 생성
echo @echo off > setup\activate.bat
echo echo 🚀 Livith Data 활성화 >> setup\activate.bat
echo call venv\Scripts\activate >> setup\activate.bat
echo echo ✅ 준비 완료! 실행: python core/pipeline/main.py >> setup\activate.bat
echo cmd /k >> setup\activate.bat

echo 🎉 설치 완료! 실행: setup\activate.bat
pause