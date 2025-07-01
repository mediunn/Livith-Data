import requests
from config import Config

def test_perplexity_connection():
    headers = {
        "Authorization": f"Bearer {Config.PERPLEXITY_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": "llama-3.1-sonar-small-128k-online",
        "messages": [{"role": "user", "content": "Hello, this is a test."}],
        "max_tokens": 50
    }
    
    try:
        response = requests.post(
            "https://api.perplexity.ai/chat/completions",
            json=payload,
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            print("API 연결 성공!")
            return True
        else:
            print(f"API 연결 실패: {response.status_code}")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"연결 오류: {e}")
        return False

if __name__ == "__main__":
    test_perplexity_connection()
