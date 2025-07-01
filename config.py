import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    PERPLEXITY_API_KEY = os.getenv('PERPLEXITY_API_KEY')
    OUTPUT_DIR = os.getenv('OUTPUT_DIR', 'output')
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    REQUEST_DELAY = int(os.getenv('REQUEST_DELAY', 1))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))
    TIMEOUT = int(os.getenv('TIMEOUT', 30))
    
    @classmethod
    def validate(cls):
        if not cls.PERPLEXITY_API_KEY:
            raise ValueError("PERPLEXITY_API_KEY가 설정되지 않았습니다.")
        return True
