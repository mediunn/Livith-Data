"""
프로젝트 설정 관리
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


class Config:
    """프로젝트 전역 설정"""
    
    # 프로젝트 경로
    PROJECT_ROOT = Path(__file__).parent.parent
    
    # API 키
    GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
    PERPLEXITY_API_KEY = os.getenv('PERPLEXITY_API_KEY')
    KOPIS_API_KEY = os.getenv('KOPIS_API_KEY')
    MUSIXMATCH_API_KEY = os.getenv('MUSIXMATCH_API_KEY')
    
    # SSH 설정
    LIVITH_SSH_KEY_PATH = os.getenv('LIVITH_SSH_KEY_PATH')
    DB_SSH_HOST = os.getenv('DB_SSH_HOST')
    DB_SSH_PORT = int(os.getenv('DB_SSH_PORT', 22))
    DB_SSH_USER = os.getenv('DB_SSH_USER')
    
    # 데이터베이스 설정
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = int(os.getenv('DB_PORT', 3306))
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_NAME = os.getenv('DB_NAME')
    
    # API 설정
    USE_GEMINI_API = os.getenv('USE_GEMINI_API', 'true').lower() == 'true'
    GEMINI_USE_SEARCH = os.getenv('GEMINI_USE_SEARCH', 'true').lower() == 'true'
    GEMINI_MODEL_VERSION = os.getenv('GEMINI_MODEL_VERSION', '2.0')
    
    # 경로 설정
    DATA_DIR = PROJECT_ROOT / "data"
    OUTPUT_DIR = DATA_DIR / "main_output"
    TEST_OUTPUT_DIR = DATA_DIR / "test_output"
    BACKUP_DIR = DATA_DIR / "backups"
    LOGS_DIR = PROJECT_ROOT / "logs"
    
    # 로깅 설정
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # API 요청 설정
    REQUEST_DELAY = int(os.getenv('REQUEST_DELAY', 2))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))
    TIMEOUT = int(os.getenv('TIMEOUT', 30))
    
    @classmethod
    def ensure_directories(cls):
        """필요한 디렉토리 생성"""
        dirs = [cls.DATA_DIR, cls.OUTPUT_DIR, cls.TEST_OUTPUT_DIR, cls.BACKUP_DIR, cls.LOGS_DIR]
        for dir_path in dirs:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def set_test_mode(cls, test_mode: bool = False):
        """테스트 모드 설정"""
        if test_mode:
            cls.OUTPUT_DIR = cls.TEST_OUTPUT_DIR
            print("🧪 테스트 모드 활성화")
        else:
            cls.OUTPUT_DIR = cls.DATA_DIR / "main_output"
            print("🚀 프로덕션 모드 활성화")
        
        cls.ensure_directories()
    
    @classmethod
    def validate_api_keys(cls):
        """필수 API 키 검증"""
        required = []
        
        if cls.USE_GEMINI_API and not cls.GEMINI_API_KEY:
            required.append('GEMINI_API_KEY')
        elif not cls.USE_GEMINI_API and not cls.PERPLEXITY_API_KEY:
            required.append('PERPLEXITY_API_KEY')
        
        if not cls.KOPIS_API_KEY:
            required.append('KOPIS_API_KEY')
        
        if required:
            raise ValueError(f"필수 환경변수가 누락됨: {', '.join(required)}")
        
        return True
    
    @classmethod
    def get_ssh_key_path(cls):
        """SSH 키 경로 가져오기"""
        if not cls.LIVITH_SSH_KEY_PATH:
            raise ValueError(
                "LIVITH_SSH_KEY_PATH 환경변수가 설정되지 않았습니다. "
                ".env.template 파일을 .env로 복사하고 경로를 설정해주세요."
            )
        
        path = Path(cls.LIVITH_SSH_KEY_PATH).expanduser().resolve()
        
        if not path.exists():
            raise FileNotFoundError(f"SSH 키 파일을 찾을 수 없습니다: {path}")
        
        return str(path)
    
    @classmethod
    def get_db_config(cls):
        """데이터베이스 설정 가져오기"""
        return {
            'ssh_host': cls.DB_SSH_HOST,
            'ssh_port': cls.DB_SSH_PORT,
            'ssh_user': cls.DB_SSH_USER,
            'key_path': cls.get_ssh_key_path(),
            'db_host': cls.DB_HOST,
            'db_port': cls.DB_PORT,
            'db_user': cls.DB_USER,
            'db_password': cls.DB_PASSWORD,
            'db_name': cls.DB_NAME
        }