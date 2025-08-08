#!/usr/bin/env python3
"""
Musixmatch API 인코딩 테스트
"""
import requests
import json
from config import Config

def test_musixmatch_encoding():
    """Musixmatch API 응답 인코딩 테스트"""
    
    # API 키 확인
    Config.validate_musixmatch()
    api_key = Config.MUSIXMATCH_API_KEY
    
    # 테스트용 트랙 검색 (일본 아티스트)
    search_url = "https://api.musixmatch.com/ws/1.1/track.search"
    params = {
        "apikey": api_key,
        "q": "羊文学",  # 히츠지분가쿠
        "page_size": 3
    }
    
    print("=" * 60)
    print("Musixmatch API 인코딩 테스트")
    print("=" * 60)
    
    try:
        # 검색 요청
        print("\n1. 트랙 검색 중...")
        response = requests.get(search_url, params=params)
        
        # 응답 인코딩 확인
        print(f"   - 응답 인코딩: {response.encoding}")
        print(f"   - 응답 헤더 Content-Type: {response.headers.get('content-type')}")
        
        # UTF-8로 강제 설정
        response.encoding = 'utf-8'
        
        # JSON 파싱
        data = response.json()
        
        if data.get("message", {}).get("header", {}).get("status_code") == 200:
            track_list = data.get("message", {}).get("body", {}).get("track_list", [])
            
            if track_list:
                track = track_list[0]["track"]
                track_id = track["track_id"]
                track_name = track["track_name"]
                artist_name = track["artist_name"]
                
                print(f"\n2. 트랙 찾음:")
                print(f"   - 트랙명: {track_name}")
                print(f"   - 아티스트: {artist_name}")
                print(f"   - 트랙 ID: {track_id}")
                
                # 가사 가져오기
                lyrics_url = "https://api.musixmatch.com/ws/1.1/track.lyrics.get"
                lyrics_params = {
                    "apikey": api_key,
                    "track_id": track_id
                }
                
                print("\n3. 가사 가져오기...")
                lyrics_response = requests.get(lyrics_url, params=lyrics_params)
                
                print(f"   - 가사 응답 인코딩: {lyrics_response.encoding}")
                
                # UTF-8로 강제 설정
                lyrics_response.encoding = 'utf-8'
                
                lyrics_data = lyrics_response.json()
                
                if lyrics_data.get("message", {}).get("header", {}).get("status_code") == 200:
                    lyrics_body = lyrics_data.get("message", {}).get("body", {}).get("lyrics", {}).get("lyrics_body", "")
                    
                    if lyrics_body:
                        print("\n4. 가사 샘플 (처음 200자):")
                        print("-" * 40)
                        print(lyrics_body[:200])
                        print("-" * 40)
                        
                        # 가사에 포함된 문자 확인
                        has_japanese = any('\u3040' <= char <= '\u309f' or '\u30a0' <= char <= '\u30ff' or '\u4e00' <= char <= '\u9fff' for char in lyrics_body)
                        has_korean = any('\uac00' <= char <= '\ud7af' for char in lyrics_body)
                        
                        print(f"\n5. 문자 분석:")
                        print(f"   - 일본어 포함: {has_japanese}")
                        print(f"   - 한국어 포함: {has_korean}")
                        print(f"   - 가사 길이: {len(lyrics_body)}자")
                    else:
                        print("\n❌ 가사가 비어있습니다")
                else:
                    print(f"\n❌ 가사 API 오류: {lyrics_data.get('message', {}).get('header', {}).get('hint', 'Unknown')}")
            else:
                print("\n❌ 트랙을 찾을 수 없습니다")
        else:
            print(f"\n❌ 검색 API 오류: {data.get('message', {}).get('header', {}).get('hint', 'Unknown')}")
            
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    test_musixmatch_encoding()