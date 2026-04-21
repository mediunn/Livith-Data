#!/usr/bin/env python3
"""
크로스 플랫폼 호환성 유틸리티
OS에 따라 subprocess 생성 방식이 다른 문제를 처리
"""
import os
import platform
import subprocess
from typing import List


class PlatformUtils:

    @staticmethod
    def is_windows() -> bool:
        return platform.system().lower() == 'windows'

    @staticmethod
    def create_subprocess(command: List[str], **kwargs) -> subprocess.Popen:
        #OS에 맞는 방식으로 subprocess 생성
        if PlatformUtils.is_windows():
            kwargs.pop('preexec_fn', None)  # Windows는 preexec_fn 지원 안 함
            if 'creationflags' not in kwargs:
                kwargs['creationflags'] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            if 'preexec_fn' not in kwargs:
                kwargs['preexec_fn'] = os.setsid  # Unix는 프로세스 그룹으로 묶어서 한번에 종료 가능

        return subprocess.Popen(command, **kwargs)


def create_cross_platform_subprocess(command: List[str], **kwargs) -> subprocess.Popen:
    #크로스 플랫폼 subprocess 생성 (db_utils에서 SSH 터널 생성 시 사용)
    return PlatformUtils.create_subprocess(command, **kwargs)
