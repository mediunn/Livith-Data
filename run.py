#!/usr/bin/env python3
import subprocess
import sys
import os

def main():
    # 프로젝트 루트 디렉토리로 이동
    project_root = os.path.dirname(os.path.abspath(__file__))
    os.chdir(project_root)
    
    # 가상환경 활성화 확인
    if not hasattr(sys, 'real_prefix') and not (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print("가상환경이 활성화되지 않았습니다.")
        print("다음 명령어를 실행하세요: source venv/bin/activate")
        return
    
    # 메인 애플리케이션 실행
    subprocess.run([sys.executable, "src/main.py"])

if __name__ == "__main__":
    main()
