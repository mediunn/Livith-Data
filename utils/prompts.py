"""
=============================================================================
중앙 집중식 프롬프트 관리 시스템
=============================================================================

이 파일은 모든 API 호출에 사용되는 프롬프트들을 중앙에서 관리합니다.
프롬프트 수정 시 이 파일만 수정하면 모든 곳에 적용됩니다.

테이블별 사용 현황:
┌─────────────────┬─────────────────────────────────────────────────────────┐
│ 테이블/컬럼     │ 사용되는 프롬프트 함수                                  │
├─────────────────┼─────────────────────────────────────────────────────────┤
│ artists.csv     │                                                         │
│  ├─ artist      │ get_artist_info_prompt()                                │
│  ├─ debut_date  │ get_artist_info_prompt()                                │
│  ├─ category    │ get_artist_info_prompt()                                │
│  ├─ detail      │ get_artist_info_prompt()                                │
│  ├─ instagram   │ get_artist_info_prompt()                                │
│  ├─ keywords    │ get_artist_info_prompt()                                │
│  └─ img_url     │ get_artist_info_prompt()                                │
├─────────────────┼─────────────────────────────────────────────────────────┤
│ concerts.csv    │                                                         │
│  ├─ artist      │ get_artist_name_prompt(), get_artist_display_prompt()   │
│  ├─ ticket_url  │ get_ticket_link_prompt()                                │
│  └─ 기타 정보    │ get_concert_info_prompt()                               │
├─────────────────┼─────────────────────────────────────────────────────────┤
│ setlists.csv    │                                                         │
│  └─ 모든 컬럼    │ get_expected_setlist_prompt(), get_actual_setlist_prompt()│
├─────────────────┼─────────────────────────────────────────────────────────┤
│ cultures.csv    │                                                         │
│  └─ 모든 컬럼    │ get_culture_info_prompt()                               │
├─────────────────┼─────────────────────────────────────────────────────────┤
│ schedule.csv    │                                                         │
│  └─ 모든 컬럼    │ get_schedule_info_prompt()                              │
├─────────────────┼─────────────────────────────────────────────────────────┤
│ md.csv          │                                                         │
│  └─ 모든 컬럼    │ get_merchandise_prompt()                                │
└─────────────────┴─────────────────────────────────────────────────────────┘

사용되는 파일:
- data_processing/enhanced_data_collector.py (메인 데이터 수집)
- src/gemini_api.py (Gemini API 시스템 프롬프트)
- src/perplexity_api.py (Perplexity API 시스템 프롬프트)
"""

class DataCollectionPrompts:
    """데이터 수집용 프롬프트 관리"""
    
    # =========================================================================
    # ARTISTS 테이블 관련 프롬프트
    # =========================================================================
    
    @staticmethod
    def get_artist_info_prompt(artist_name: str) -> str:
        """
        사용 위치: data_processing/enhanced_data_collector.py -> _collect_artist_info()
        목적: artists.csv의 모든 컬럼 정보 수집
        테이블: artists.csv
        컬럼: artist, debut_date, category, detail, instagram_url, keywords, img_url
        """
        return f"""아래 아티스트의 정보를 정확하게 검색해서 JSON 형태로 제공해주세요.

검색할 아티스트: {artist_name}

반드시 위에 명시된 "{artist_name}" 아티스트의 정보만 찾아주세요. 다른 아티스트의 정보는 절대 포함하지 마세요.

중요 규칙:
1. artist: "원어 (한국어)" 형식으로 작성해주세요. 예: "IU (아이유)", "BTS (방탄소년단)"
2. debut_date: 데뷔연도를 YYYY 형식의 문자열로 작성해주세요 (예: "2010", "2008")
3. detail: 아티스트 자체에 대한 정보만 포함하세요 (해요체 사용):
   - 활동 지역/국가 (어디서 활동하는지)
   - 음악 스타일과 장르
   - 아티스트명/그룹명의 의미
   - 그룹인 경우: 멤버 구성 (이름, 역할)
   - 데뷔 과정과 배경
   - 주요 음악적 특징이나 성취
   - 해당 아티스트만의 특징 등
   - 아티스트와 관련이 있으며, 독자들이 흥미있어 할 내용들
   - 절대 포함하지 말 것: 내한 콘서트, 한국 활동, 검색되지 않는 정보, 출처 표시([1], [2], URL 등)
4. keywords: 장르, 스타일, 특징만 포함하고 아티스트 이름은 절대 포함하지 마세요 (예: "록,팝,발라드")
5. img_url: 가장 대표적이고 고화질인 공식 프로필 사진 URL을 찾아주세요

JSON 형식으로만 답변:
{{"artist": "원어 (한국어) 형식", "debut_date": "데뷔연도(YYYY) 또는 빈문자열", "category": "아티스트 카테고리 또는 빈문자열", "detail": "해요체로 작성된 깔끔한 설명 (출처 표시 없음)", "instagram_url": "인스타그램URL 또는 빈문자열", "keywords": "장르나 특징만 (아티스트명 제외)", "img_url": "가장 대표적인 고화질 프로필이미지URL 또는 빈문자열"}}

JSON만 반환하고, 반드시 "{artist_name}" 아티스트의 정보만 제공하세요."""

    # =========================================================================
    # CONCERTS 테이블 관련 프롬프트
    # =========================================================================
    
    @staticmethod
    def get_artist_name_prompt(concert_title: str) -> str:
        """
        사용 위치: data_processing/enhanced_data_collector.py -> _extract_artist_name()
        목적: concerts.csv의 artist 컬럼 정보 추출
        테이블: concerts.csv
        컬럼: artist
        """
        return f""""{concert_title}" 콘서트의 정확한 아티스트 이름을 검색해주세요.

규칙:
1. 반드시 정확한 아티스트명만 반환
2. 콘서트 제목에서 추출하지 말고 웹에서 검색해서 확인
3. "원어 (한국어)" 형식으로 작성 (예: "IU (아이유)", "BTS (방탄소년단)")
4. 아티스트명만 반환하고 다른 설명은 포함하지 마세요

아티스트명: """

    @staticmethod
    def get_artist_display_prompt(concert_title: str, artist_name: str, kopis_artist: str) -> str:
        """
        사용 위치: data_processing/enhanced_data_collector.py -> _collect_artist_display_name()
        목적: concerts.csv의 artist 컬럼 정보 보정
        테이블: concerts.csv  
        컬럼: artist
        """
        return f""""{concert_title}" 콘서트의 아티스트 정보를 검색해서 정확한 표기명을 찾아주세요.

콘서트 제목: {concert_title}
추출된 아티스트명: {artist_name}
KOPIS 아티스트명: {kopis_artist}

올바른 아티스트 표기명을 "원어 (한국어)" 형식으로 찾아서 반환해주세요.
예: "IU (아이유)", "BTS (방탄소년단)", "Coldplay (콜드플레이)"

정확한 아티스트 표기명: """

    @staticmethod
    def get_ticket_link_prompt(artist_name: str, concert_title: str) -> str:
        """
        사용 위치: data_processing/enhanced_data_collector.py -> _search_ticket_link()
        목적: concerts.csv의 ticket_url 컬럼 정보 수집
        테이블: concerts.csv
        컬럼: ticket_url
        """
        return f""""{artist_name}"의 "{concert_title}" 콘서트 정확한 예매 링크를 찾아주세요.

요구사항:
1. 공식 티켓 판매처 링크만 제공 (인터파크, 멜론티켓, 예스24 등)
2. 직접 예매 가능한 상세 페이지 URL
3. 링크가 존재하지 않으면 "링크 없음" 반환

예매 링크: """

    @staticmethod
    def get_concert_info_prompt(artist_name: str, concert_title: str) -> str:
        """
        사용 위치: data_processing/enhanced_data_collector.py -> _collect_concert_info()
        목적: concerts.csv의 기타 정보 수집
        테이블: concerts.csv
        컬럼: 기타 필요한 정보들
        """
        return f"""{artist_name}의 "{concert_title}" 콘서트의 중요한 정보를 검색해주세요.

필요한 정보:
- 공연장 정보
- 공연 일정
- 티켓 가격대
- 특별한 공연 정보

JSON 형식으로 반환해주세요."""

    # =========================================================================
    # SETLISTS 테이블 관련 프롬프트
    # =========================================================================
    
    @staticmethod
    def get_expected_setlist_prompt(artist_name: str, concert_title: str) -> str:
        """
        사용 위치: data_processing/enhanced_data_collector.py -> _collect_setlists() (예상)
        목적: setlists.csv의 예상 셋리스트 정보 생성
        테이블: setlists.csv, setlist_songs.csv
        컬럼: 모든 컬럼
        """
        return f"""🚨 중요: {artist_name}의 콘서트 예상 셋리스트를 15곡 이상 반드시 만들어주세요! 🚨

콘서트: {artist_name}의 "{concert_title}"

조건:
1. {artist_name}의 대표곡들을 중심으로 구성
2. 최소 15곡 이상, 최대 25곡
3. 실제 콘서트에서 연주 가능성이 높은 곡들로만 구성
4. 곡 순서도 실제 콘서트 흐름을 고려해서 배치
5. 앙코르 곡도 포함

JSON 배열 형식으로 반환:
[{{"order": 1, "song_title": "곡제목"}}, {{"order": 2, "song_title": "곡제목"}}, ...]"""

    @staticmethod  
    def get_actual_setlist_prompt(artist_name: str, concert_title: str, venue: str, date: str) -> str:
        """
        사용 위치: data_processing/enhanced_data_collector.py -> _collect_setlists() (실제)
        목적: setlists.csv의 실제 셋리스트 정보 수집
        테이블: setlists.csv, setlist_songs.csv
        컬럼: 모든 컬럼
        """
        return f"""다음 아티스트의 콘서트에서 실제로 연주한 셋리스트를 전 세계적으로 검색해주세요.

아티스트: {artist_name}
콘서트: {concert_title}
장소: {venue}
날짜: {date}

요구사항:
1. 정확한 셋리스트만 제공 (추측 금지)
2. 곡 순서와 제목을 정확히 기재
3. 앙코르 곡도 포함
4. 실제 정보가 없으면 "정보 없음" 반환

JSON 배열 형식:
[{{"order": 1, "song_title": "실제곡제목"}}, {{"order": 2, "song_title": "실제곡제목"}}, ...]"""

    # =========================================================================
    # CULTURES 테이블 관련 프롬프트
    # =========================================================================
    
    @staticmethod
    def get_culture_info_prompt(artist_name: str, concert_title: str) -> str:
        """
        사용 위치: data_processing/enhanced_data_collector.py -> _collect_culture_info()
        목적: cultures.csv의 문화 정보 수집
        테이블: cultures.csv
        컬럼: 모든 컬럼
        """
        return f"""{artist_name}의 "{concert_title}" 콘서트만의 독특하고 고유한 문화적 특징을 검색해주세요.

찾아야 할 문화 정보:
1. 팬덤 문화 (응원 방식, 특별한 이벤트)
2. 콘서트장 분위기와 독특한 관습
3. 아티스트와 팬들 간의 특별한 소통 방식
4. 이 콘서트만의 특별한 전통이나 의식
5. 지역적 특색이나 한국적 요소

JSON 배열 형식으로 반환:
[{{"category": "문화카테고리", "content": "구체적인 문화 내용", "img_url": "관련이미지URL또는빈문자열"}}]"""

    # =========================================================================
    # SCHEDULE 테이블 관련 프롬프트
    # =========================================================================
    
    @staticmethod
    def get_schedule_info_prompt(artist_name: str, concert_title: str, start_date: str, end_date: str) -> str:
        """
        사용 위치: data_processing/enhanced_data_collector.py -> _collect_schedule_info()
        목적: schedule.csv의 일정 정보 수집
        테이블: schedule.csv
        컬럼: 모든 컬럼
        """
        return f"""{artist_name}의 "{concert_title}" 콘서트 관련 모든 일정을 {start_date}부터 {end_date}까지 검색해주세요.

포함할 일정:
1. 티켓 예매 오픈 일정
2. 콘서트 본 공연 일정 (모든 회차)
3. 리허설이나 사운드 체크 일정
4. 팬미팅이나 부대 행사 일정
5. 굿즈 판매 일정

JSON 배열 형식으로 반환:
[{{"date": "YYYY-MM-DD", "time": "HH:MM", "event": "이벤트명", "location": "장소", "note": "추가정보또는빈문자열"}}]"""

    # =========================================================================
    # MD (MERCHANDISE) 테이블 관련 프롬프트
    # =========================================================================
    
    @staticmethod
    def get_merchandise_prompt(artist_name: str, concert_title: str) -> str:
        """
        사용 위치: data_processing/enhanced_data_collector.py -> _collect_merchandise_info()
        목적: md.csv의 굿즈 정보 수집
        테이블: md.csv
        컬럼: 모든 컬럼
        """
        return f""""{artist_name}"의 "{concert_title}" 콘서트 굿즈 판매 현황과 한정판 정보를 검색해주세요:

1. 공식 굿즈 목록과 가격
2. 한정판이나 특별 제작 아이템
3. 콘서트장 전용 굿즈
4. 온라인 판매 여부
5. 품절 현황

JSON 배열 형식:
[{{"item_name": "굿즈명", "price": "가격", "availability": "판매상태", "description": "상품설명", "img_url": "이미지URL또는빈문자열"}}]"""


class APIPrompts:
    """API 시스템 프롬프트 관리"""
    
    # =========================================================================
    # GEMINI API 시스템 프롬프트
    # =========================================================================
    
    @staticmethod
    def get_gemini_system_prompt() -> str:
        """
        사용 위치: src/gemini_api.py -> query_with_search()
        목적: Gemini API 호출 시 기본 시스템 프롬프트
        """
        return """당신은 Google Search를 활용하여 최신 한국 내한 콘서트 정보를 수집하는 전문가입니다.

핵심 원칙:
1. 반드시 Google Search를 통해 검색된 최신 정보만 제공
2. 추측이나 추론은 절대 금지
3. 정확하지 않은 정보는 제공하지 않음
4. 한국어로 자연스럽게 답변
5. 요청된 형식(JSON 등)을 정확히 준수

검색 우선순위:
- 공식 발표 > 공식 티켓 사이트 > 신뢰할 수 있는 언론사 > 기타 소스"""

    @staticmethod
    def get_gemini_json_prompt(prompt: str) -> str:
        """
        사용 위치: src/gemini_api.py -> query_json()
        목적: JSON 응답 보장을 위한 프롬프트 래퍼
        """
        return f"""{prompt}

중요: 응답은 반드시 유효한 JSON 형식으로만 제공하세요. 설명이나 추가 텍스트는 포함하지 마세요."""

    @staticmethod
    def get_gemini_url_context_prompt(prompt: str, urls: list) -> str:
        """
        사용 위치: src/gemini_api.py -> query_with_search()
        목적: 특정 URL 컨텍스트를 포함한 검색
        """
        url_list = ", ".join(urls)
        return f"""{prompt}

참조할 URL: {url_list}

위 URL들의 내용을 우선적으로 참조하여 답변해주세요."""

    # =========================================================================
    # PERPLEXITY API 시스템 프롬프트  
    # =========================================================================
    
    @staticmethod
    def get_perplexity_system_prompt(prompt: str) -> str:
        """
        사용 위치: src/perplexity_api.py -> query()
        목적: Perplexity API 호출 시 기본 시스템 프롬프트 래퍼
        """
        return f"""중요: 웹 검색을 통해 찾은 정보만 제공해야 합니다. 추측이나 추론은 절대 하지 마세요.

검색 요청:
{prompt}

답변 규칙:
1. 검색된 실제 정보만 제공
2. 정보를 찾을 수 없으면 "정보를 찾을 수 없습니다" 명시
3. 한국어로 자연스럽게 답변
4. 요청된 형식을 정확히 준수"""


# =============================================================================
# 하위 호환성을 위한 별칭 (기존 코드가 동작하도록)
# =============================================================================

class ArtistPrompts:
    """기존 artist_prompts.py와의 하위 호환성"""
    
    @staticmethod
    def get_artist_info_prompt(artist_name: str) -> str:
        return DataCollectionPrompts.get_artist_info_prompt(artist_name)
    
    @staticmethod 
    def get_artist_enhancement_prompt(artist_name: str, empty_fields: list) -> str:
        # 향후 필요시 구현 예정
        return DataCollectionPrompts.get_artist_info_prompt(artist_name)
