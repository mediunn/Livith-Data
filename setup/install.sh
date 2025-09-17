#!/bin/bash
# Livith Data 설치 스크립트

set -e
echo "🚀 Livith Data 설치 시작..."

# Python 확인
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 필요"
    exit 1
fi

# 가상환경 생성
echo "🔧 가상환경 생성..."
rm -rf venv
python3 -m venv venv

# 의존성 설치
echo "📦 패키지 설치..."
source venv/bin/activate
pip install --upgrade pip
pip install -r setup/requirements.txt
pip install sshtunnel

# 환경 파일 복사
if [ ! -f .env ] && [ -f .env.example ]; then
    cp .env.example .env
    echo "✅ .env 파일 생성"
fi

# 활성화 스크립트 생성
cat > setup/activate.sh << 'EOF'
#!/bin/bash
echo "🚀 Livith Data 활성화"
source venv/bin/activate
echo "✅ 준비 완료! 실행: python core/pipeline/main.py"
exec "$SHELL"
EOF
chmod +x setup/activate.sh

echo "🎉 설치 완료! 실행: source setup/activate.sh"