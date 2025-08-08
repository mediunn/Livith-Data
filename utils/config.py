import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    PERPLEXITY_API_KEY = os.getenv('PERPLEXITY_API_KEY')
    KOPIS_API_KEY = os.getenv('KOPIS_API_KEY')
    MUSIXMATCH_API_KEY = os.getenv('MUSIXMATCH_API_KEY')
    OUTPUT_DIR = os.getenv('OUTPUT_DIR', 'output')
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    REQUEST_DELAY = int(os.getenv('REQUEST_DELAY', 2))
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
        
        if not cls.PERPLEXITY_API_KEY:
            missing_keys.append('PERPLEXITY_API_KEY')
        
        if not cls.KOPIS_API_KEY:
            missing_keys.append('KOPIS_API_KEY')
        
        if check_musixmatch and not cls.MUSIXMATCH_API_KEY:
            missing_keys.append('MUSIXMATCH_API_KEY')
        
        if missing_keys:
            raise ValueError(f"다음 환경변수가 설정되지 않았습니다: {', '.join(missing_keys)}")
        
        return True
    
    @classmethod
    def validate_musixmatch(cls):
        """Musixmatch API 키만 검증"""
        if not cls.MUSIXMATCH_API_KEY:
            raise ValueError("MUSIXMATCH_API_KEY 환경변수가 설정되지 않았습니다.")
        return True
    
