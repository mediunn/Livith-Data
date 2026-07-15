#!/bin/bash
# Instagram 크롤링 crontab 실행 스크립트
# 사용법: crontab에 아래 추가
#   0 8,20 * * * /path/to/livith-Data/scripts/run_instagram_cron.sh >> /path/to/livith-Data/logs/instagram_cron.log 2>&1

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
VENV_DIR="$PROJECT_DIR/.venv"
LOG_DIR="$PROJECT_DIR/logs"
DB_TARGET="--dev"   # livith_service 전환 시 --prod 또는 --service 로 변경

mkdir -p "$LOG_DIR"

echo "========================================"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Instagram 크롤링 시작 (DB: $DB_TARGET)"
echo "========================================"

# 가상환경 활성화
if [ -f "$VENV_DIR/bin/activate" ]; then
    source "$VENV_DIR/bin/activate"
else
    echo "❌ 가상환경을 찾을 수 없습니다: $VENV_DIR"
    exit 1
fi

cd "$PROJECT_DIR"

python tools/data/run_instagram_pipeline.py $DB_TARGET
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ 완료"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ❌ 오류 발생 (exit code: $EXIT_CODE)"
fi

echo ""
exit $EXIT_CODE
