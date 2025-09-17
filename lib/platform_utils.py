#!/usr/bin/env python3
"""
크로스 플랫폼 호환성을 위한 유틸리티 함수들
"""
import os
import platform
import subprocess
from typing import List, Optional, Any, Dict


class PlatformUtils:
    """플랫폼별 차이점을 처리하는 유틸리티 클래스"""
    
    @staticmethod
    def get_platform() -> str:
        """현재 플랫폼 반환"""
        return platform.system().lower()
    
    @staticmethod
    def is_windows() -> bool:
        """윈도우 플랫폼인지 확인"""
        return platform.system().lower() == 'windows'
    
    @staticmethod
    def is_mac() -> bool:
        """macOS 플랫폼인지 확인"""
        return platform.system().lower() == 'darwin'
    
    @staticmethod
    def is_linux() -> bool:
        """리눅스 플랫폼인지 확인"""
        return platform.system().lower() == 'linux'
    
    @staticmethod
    def create_subprocess(
        command: List[str],
        **kwargs
    ) -> subprocess.Popen:
        """
        플랫폼별로 최적화된 subprocess 생성
        
        Args:
            command: 실행할 명령어 리스트
            **kwargs: subprocess.Popen에 전달할 추가 인자
            
        Returns:
            subprocess.Popen 객체
        """
        # 윈도우에서는 preexec_fn 사용 불가
        if PlatformUtils.is_windows():
            # Windows에서는 preexec_fn 제거
            kwargs.pop('preexec_fn', None)
            # Windows에서 SSH 터널 생성 시 별도 처리 필요
            if 'creationflags' not in kwargs:
                kwargs['creationflags'] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            # Unix 계열에서는 preexec_fn=os.setsid 사용 가능
            if 'preexec_fn' not in kwargs:
                kwargs['preexec_fn'] = os.setsid
        
        return subprocess.Popen(command, **kwargs)
    
    @staticmethod
    def get_ssh_command(
        ssh_host: str,
        ssh_user: str,
        ssh_key_path: str,
        local_port: int,
        remote_host: str,
        remote_port: int
    ) -> List[str]:
        """
        플랫폼별 SSH 터널 명령어 생성
        
        Args:
            ssh_host: SSH 호스트
            ssh_user: SSH 사용자명
            ssh_key_path: SSH 키 파일 경로
            local_port: 로컬 포트
            remote_host: 원격 호스트
            remote_port: 원격 포트
            
        Returns:
            SSH 명령어 리스트
        """
        if PlatformUtils.is_windows():
            # Windows에서는 OpenSSH 또는 PuTTY 사용
            # OpenSSH가 설치되어 있다고 가정
            return [
                'ssh',
                '-i', ssh_key_path,
                '-L', f'{local_port}:{remote_host}:{remote_port}',
                '-N',
                '-o', 'StrictHostKeyChecking=no',
                '-o', 'UserKnownHostsFile=NUL',  # Windows용
                f'{ssh_user}@{ssh_host}'
            ]
        else:
            # Unix 계열 (macOS, Linux)
            return [
                'ssh',
                '-i', ssh_key_path,
                '-L', f'{local_port}:{remote_host}:{remote_port}',
                '-N',
                '-o', 'StrictHostKeyChecking=no',
                '-o', 'UserKnownHostsFile=/dev/null',  # Unix용
                f'{ssh_user}@{ssh_host}'
            ]
    
    @staticmethod
    def get_default_paths() -> Dict[str, str]:
        """
        플랫폼별 기본 경로 반환
        
        Returns:
            경로 딕셔너리
        """
        if PlatformUtils.is_windows():
            return {
                'home': os.path.expanduser('~'),
                'ssh_keys': os.path.join(os.path.expanduser('~'), '.ssh'),
                'config': os.path.join(os.path.expanduser('~'), 'AppData', 'Local'),
                'temp': os.environ.get('TEMP', 'C:\\temp'),
            }
        else:
            # Unix 계열 (macOS, Linux)
            return {
                'home': os.path.expanduser('~'),
                'ssh_keys': os.path.join(os.path.expanduser('~'), '.ssh'),
                'config': os.path.join(os.path.expanduser('~'), '.config'),
                'temp': '/tmp',
            }
    
    @staticmethod
    def normalize_path(path: str) -> str:
        """
        플랫폼에 맞게 경로 정규화
        
        Args:
            path: 정규화할 경로
            
        Returns:
            정규화된 경로
        """
        # 환경 변수 치환
        path = os.path.expandvars(path)
        # 홈 디렉토리 치환
        path = os.path.expanduser(path)
        # 절대 경로로 변환
        path = os.path.abspath(path)
        # 플랫폼별 경로 구분자 정규화
        return os.path.normpath(path)
    
    @staticmethod
    def kill_process_group(process: subprocess.Popen) -> None:
        """
        플랫폼별로 프로세스 그룹 종료
        
        Args:
            process: 종료할 프로세스
        """
        try:
            if PlatformUtils.is_windows():
                # Windows에서는 taskkill 사용
                subprocess.run([
                    'taskkill', '/F', '/T', '/PID', str(process.pid)
                ], check=False, capture_output=True)
            else:
                # Unix 계열에서는 kill 사용
                os.killpg(os.getpgid(process.pid), 9)
        except (OSError, subprocess.SubprocessError):
            # 프로세스가 이미 종료된 경우 무시
            pass
    
    @staticmethod
    def get_python_executable() -> str:
        """
        현재 플랫폼에서 사용할 Python 실행 파일 경로 반환
        
        Returns:
            Python 실행 파일 경로
        """
        import sys
        return sys.executable


# 편의를 위한 전역 함수들
def is_windows() -> bool:
    """윈도우 플랫폼인지 확인"""
    return PlatformUtils.is_windows()

def is_mac() -> bool:
    """macOS 플랫폼인지 확인"""
    return PlatformUtils.is_mac()

def is_linux() -> bool:
    """리눅스 플랫폼인지 확인"""
    return PlatformUtils.is_linux()

def create_cross_platform_subprocess(command: List[str], **kwargs) -> subprocess.Popen:
    """크로스 플랫폼 subprocess 생성"""
    return PlatformUtils.create_subprocess(command, **kwargs)