import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    PERPLEXITY_API_KEY = os.getenv('PERPLEXITY_API_KEY')
    KOPIS_API_KEY = os.getenv('KOPIS_API_KEY')
    OUTPUT_DIR = os.getenv('OUTPUT_DIR', 'output')
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    REQUEST_DELAY = int(os.getenv('REQUEST_DELAY', 2))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))
    TIMEOUT = int(os.getenv('TIMEOUT', 30))
    
    @classmethod
    def validate(cls):
        missing_keys = []
        
        if not cls.PERPLEXITY_API_KEY:
            missing_keys.append('PERPLEXITY_API_KEY')
        
        if not cls.KOPIS_API_KEY:
            missing_keys.append('KOPIS_API_KEY')
        
        if missing_keys:
            raise ValueError(f"다음 환경변수가 설정되지 않았습니다: {', '.join(missing_keys)}")
        
        return True
    
