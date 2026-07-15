# GCP 자동화 세팅 가이드

## 1. 초기 세팅 (1회)

```bash
git clone [repo] livith-Data
cd livith-Data
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# .env 파일 생성 (로컬 .env 내용 복사)
nano .env

# 스크립트 실행 권한 부여
chmod +x scripts/run_instagram_cron.sh

# logs 폴더 생성
mkdir -p logs
```

## 2. Instagram 세션 최초 로그인 (1회)

```bash
source .venv/bin/activate
python tools/data/run_instagram_pipeline.py --dev
```

세션 파일이 `data/instagram_session/`에 저장되면 이후 자동 재사용됨.

## 3. crontab 등록

```bash
crontab -e
```

아래 추가 (매일 오전 8시, 오후 8시 실행):

```
0 8,20 * * * /home/[user]/livith-Data/scripts/run_instagram_cron.sh >> /home/[user]/livith-Data/logs/instagram_cron.log 2>&1
```

## 4. 로그 확인

```bash
tail -f logs/instagram_cron.log
```

## 5. livith_service 전환 시

`scripts/run_instagram_cron.sh` 파일에서:
```bash
DB_TARGET="--dev"   # 이 줄을
DB_TARGET="--prod"  # 이렇게 변경
```
