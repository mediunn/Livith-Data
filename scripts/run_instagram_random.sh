#!/bin/bash
# 오전 (9~11시 사이 랜덤 딜레이)
DELAY=$(( RANDOM % 7200 ))
sleep $DELAY 
cd /home/silverlives0915/livith-data && ./venv/bin/python tools/data/run_instagram_pipeline.py --prod >>/home/silverlives0915/logs/instagram.log 2>&1
