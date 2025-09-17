import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # API 키 설정
    PERPLEXITY_API_KEY = os.getenv('PERPLEXITY_API_KEY')
    GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
    KOPIS_API_KEY = os.getenv('KOPIS_API_KEY')
    MUSIXMATCH_API_KEY = os.getenv('MUSIXMATCH_API_KEY')
    
    # API 선택 (기본값: Gemini 사용)
    USE_GEMINI_API = os.getenv('USE_GEMINI_API', 'true').lower() == 'true'
    
    # Gemini 검색 설정
    GEMINI_USE_SEARCH = os.getenv('GEMINI_USE_SEARCH', 'true').lower() == 'true'
    GEMINI_MODEL_VERSION = os.getenv('GEMINI_MODEL_VERSION', '2.0')
    
    # 디렉토리 및 로깅 설정
    BASE_OUTPUT_DIR = os.getenv('OUTPUT_DIR', 'output')
    MAIN_OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, 'main_output')
    TEST_OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, 'test_output')
    BACKUP_DIR = os.path.join(BASE_OUTPUT_DIR, 'backups')
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # 현재 실행 모드에 따른 출력 디렉토리 (동적으로 설정됨)
    OUTPUT_DIR = MAIN_OUTPUT_DIR  # 기본값
    
    @classmethod
    def set_output_mode(cls):
        """OUTPUT_MODE 환경변수에 따라 출력 디렉토리 설정"""
        output_mode = os.getenv('OUTPUT_MODE', 'production')
        if output_mode == 'test':
            cls.OUTPUT_DIR = cls.TEST_OUTPUT_DIR
            print(f"🧪 테스트 모드: {cls.OUTPUT_DIR} 사용")
        else:
            cls.OUTPUT_DIR = cls.MAIN_OUTPUT_DIR
            print(f"🚀 프로덕션 모드: {cls.OUTPUT_DIR} 사용")
    
    # API 요청 설정
    REQUEST_DELAY = int(os.getenv('REQUEST_DELAY', 5))  # 2초 → 5초로 증가
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))
    TIMEOUT = int(os.getenv('TIMEOUT', 30))
    
    @classmethod
    def validate(cls, check_musixmatch: bool = False):
        """
        환경변수 검증
        Args:
            check_musixmatch: True면 MUSIXMATCH_API_KEY도 필수로 검증
        """
        missing_keys = []
        
        # API 선택에 따른 검증
        if cls.USE_GEMINI_API:
            if not cls.GEMINI_API_KEY:
                missing_keys.append('GEMINI_API_KEY')
        else:
            if not cls.PERPLEXITY_API_KEY:
                missing_keys.append('PERPLEXITY_API_KEY')
        
        # KOPIS는 항상 필수
        if not cls.KOPIS_API_KEY:
            missing_keys.append('KOPIS_API_KEY')
        
        # Musixmatch는 선택적
        if check_musixmatch and not cls.MUSIXMATCH_API_KEY:
            missing_keys.append('MUSIXMATCH_API_KEY')
        
        if missing_keys:
            api_type = "Gemini" if cls.USE_GEMINI_API else "Perplexity"
            raise ValueError(f"다음 환경변수가 설정되지 않았습니다: {', '.join(missing_keys)}\n"
                           f"(현재 {api_type} API 사용 중)")
        
        return True
    
    @classmethod
    def validate_musixmatch(cls):
        """Musixmatch API 키만 검증"""
        if not cls.MUSIXMATCH_API_KEY:
            raise ValueError("MUSIXMATCH_API_KEY 환경변수가 설정되지 않았습니다.")
        return True
    
    @classmethod
    def set_test_mode(cls, test_mode: bool = False):
        """
        테스트 모드에 따라 출력 디렉토리를 설정
        Args:
            test_mode: True면 test_output, False면 main_output 사용
        """
        if test_mode:
            cls.OUTPUT_DIR = cls.TEST_OUTPUT_DIR
        else:
            cls.OUTPUT_DIR = cls.MAIN_OUTPUT_DIR
        
        # 디렉토리 생성
        cls._ensure_directories()
    
    @classmethod
    def _ensure_directories(cls):
        """필요한 디렉토리들을 생성"""
        directories = [cls.MAIN_OUTPUT_DIR, cls.TEST_OUTPUT_DIR, cls.BACKUP_DIR]
        
        for directory in directories:
            if not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
    
    @classmethod
    def create_backup(cls, filename: str) -> str:
        """
        메인 출력 디렉토리의 파일을 백업
        Args:
            filename: 백업할 파일명
        Returns:
            백업 파일의 전체 경로
        """
        from datetime import datetime
        
        original_file = os.path.join(cls.MAIN_OUTPUT_DIR, filename)
        
        if not os.path.exists(original_file):
            return None
        
        # 백업 파일명: filename_YYYYMMDD_HHMMSS.ext
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        name, ext = os.path.splitext(filename)
        backup_filename = f"{name}_{timestamp}{ext}"
        backup_path = os.path.join(cls.BACKUP_DIR, backup_filename)
        
        # 파일 복사
        import shutil
        shutil.copy2(original_file, backup_path)
        
        return backup_path
    
    @classmethod 
    def get_backup_files(cls, filename: str) -> list:
        """
        특정 파일의 백업 목록을 조회
        Args:
            filename: 원본 파일명
        Returns:
            백업 파일 경로 리스트 (최신순)
        """
        import glob
        
        name, ext = os.path.splitext(filename)
        pattern = os.path.join(cls.BACKUP_DIR, f"{name}_*{ext}")
        backup_files = glob.glob(pattern)
        
        # 파일명으로 정렬 (최신순)
        backup_files.sort(reverse=True)
        
        return backup_files
    
