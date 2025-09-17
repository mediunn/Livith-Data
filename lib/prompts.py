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
    # 🚨 모든 프롬프트 공통 규칙 (매우 중요!)
    # =========================================================================
    COMMON_SOURCE_RULES = """
🚫 **절대 금지사항 (모든 데이터 수집 공통):**
- 추측, 예상, 가정에 기반한 정보 생성 금지
- "보통", "일반적으로", "아마도", "대개", "추정" 등의 표현 사용 금지
- 출처를 확인할 수 없는 정보 포함 금지
- 다른 아티스트나 공연의 정보로 유추하여 작성 금지
- 일반적인 상식이나 패턴으로 정보 생성 금지

✅ **반드시 준수사항:**
- 공식 출처에서 확인된 정보만 포함 (공식 웹사이트, SNS, 신뢰할 수 있는 언론 보도)
- 확실하지 않은 정보는 빈 값("") 또는 빈 배열([]) 반환
- 구체적이고 사실에 기반한 정보만 제공
- 출처가 명확하지 않으면 "정보를 찾을 수 없습니다" 보다는 빈 값 반환

⚠️ **확실하지 않으면 포함하지 마세요! 빈 값이 잘못된 정보보다 낫습니다!**
"""
    
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
        return f"""{DataCollectionPrompts.COMMON_SOURCE_RULES}

아래 아티스트의 **실제 확인된** 정보만을 정확하게 검색해서 JSON 형태로 제공해주세요.

검색할 아티스트: {artist_name}

반드시 위에 명시된 "{artist_name}" 아티스트의 정보만 찾아주세요. 다른 아티스트의 정보는 절대 포함하지 마세요.

⭐ 필수 규칙: 아티스트명 통일 ⭐
- artist 필드는 반드시 "{artist_name}" 그대로 사용 (절대 변경 금지)
- concerts 테이블과 동일한 표기 유지
- 다른 표기나 영문명으로 변경하지 말고 입력된 그대로 사용

중요 규칙:
1. artist: "{artist_name}" 그대로 사용
2. debut_date: 데뷔연도 또는 첫 앨범 출시 연도를 YYYY 형식의 문자열로 작성해주세요 (예: "2010", "2008")
3. detail: 아티스트 자체에 대한 정보만 포함하세요 (~요, ~니다를 섞어 친근하게 작성):
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

📝 문체 규칙:
- ~요, ~니다를 섞어 친근하게 작성
- "~이에요" 등 문법에 어긋나는 말 사용 절대 금지
- 정확한 한국어 문법과 맞춤법 준수

JSON 형식으로만 답변:
{{"artist": "원어 (한국어) 형식", "debut_date": "데뷔연도(YYYY) 또는 빈문자열", "category": "아티스트 카테고리 또는 빈문자열", "detail": "~요, ~니다를 섞어 친근하게 작성된 정확한 설명 (출처 표시 없음)", "instagram_url": "인스타그램URL 또는 빈문자열", "keywords": "장르나 특징만 (아티스트명 제외)", "img_url": "가장 대표적인 고화질 프로필이미지URL 또는 빈문자열"}}

JSON만 반환하고, 반드시 "{artist_name}" 아티스트의 정보만 제공하세요."""

    # =========================================================================
    # CONCERTS 테이블 관련 프롬프트
    # =========================================================================
    
    @staticmethod
    def get_artist_name_prompt(concert_title: str) -> str:
        """
        사용 위치: data_processing/enhanced_data_collector.py -> _search_artist_from_concert()
        목적: concerts.csv의 artist 컬럼 정보 추출
        테이블: concerts.csv
        컬럼: artist
        """
        return f"""콘서트 제목 "{concert_title}"에서 아티스트/밴드 이름을 추출해서 "원어 (한국어)" 형식으로 변환해주세요.

🎯 작업:
콘서트 제목에 포함된 아티스트명을 찾아서 "원어 (한국어)" 형식으로 변환해주세요.

📋 **필수 형식: "원어 (한국어)"**
- 원어: 공식 영문명, 일본어명, 또는 원래 표기
- 한국어: 한국에서 통용되는 한국어 표기

📝 **올바른 변환 예시:**
- "오아시스 내한공연 [고양]" → "Oasis (오아시스)"
- "BTS 월드투어 [서울]" → "BTS (방탄소년단)"  
- "아이유 콘서트 [부산]" → "IU (아이유)"
- "폼파돌스 라이브 [서울]" → "PompadollS (폼파돌스)"
- "MADKID 투어 [대구]" → "MADKID (매드키드)"

❌ **절대 사용 금지:**
- "아티스트명", "가수 이름", "밴드명" 등의 일반적 용어
- 괄호 없이 한국어만 또는 영어만 사용

⚠️ 중요: 실제 공연 여부나 해체 여부는 고려하지 마세요. 단순히 제목에서 아티스트명만 추출하여 올바른 형식으로 변환하세요.

반드시 JSON으로 응답:
{{"artist": "원어 (한국어)"}}"""

    @staticmethod
    def get_artist_display_prompt(concert_title: str, artist_name: str, kopis_artist: str) -> str:
        """
        사용 위치: data_processing/enhanced_data_collector.py -> _collect_artist_display_name()
        목적: concerts.csv의 artist 컬럼 정보 보정
        테이블: concerts.csv  
        컬럼: artist
        """
        return f"""{DataCollectionPrompts.COMMON_SOURCE_RULES}

"{concert_title}" 콘서트의 아티스트 정보를 **공식 출처에서** 검색해서 정확한 표기명을 찾아주세요.

콘서트 제목: {concert_title}
추출된 아티스트명: {artist_name}
KOPIS 아티스트명: {kopis_artist}

📋 **필수 형식: "원어 (한국어)"**
- 원어: 공식 영문명, 일본어명, 또는 원래 표기
- 한국어: 한국에서 통용되는 한국어 표기

✅ **올바른 예시:**
- "IU (아이유)"
- "BTS (방탄소년단)" 
- "Coldplay (콜드플레이)"
- "Hitsujibungaku (히츠지분가쿠)"
- "ONE OK ROCK (원 오케이 락)"

❌ **잘못된 예시:**
- "아티스트명" (절대 사용 금지)
- "가수 이름" (절대 사용 금지)
- "밴드명" (절대 사용 금지)
- 괄호 없이 한국어만: "아이유"
- 괄호 없이 영어만: "Coldplay"

🔍 **확인 방법:**
1. 공식 웹사이트, 위키피디아에서 정확한 원어 표기 확인
2. 한국 언론사, 음악 매체에서 한국어 표기 확인
3. 공식 SNS에서 사용하는 표기 확인

**JSON 형식으로 응답:**
{{"artist": "원어 (한국어)"}}"""

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
3. 링크가 존재하지 않으면 빈 문자열 반환

예매 링크: """

    @staticmethod
    def get_concert_info_prompt(artist_name: str, concert_title: str) -> str:
        """
        사용 위치: data_processing/enhanced_data_collector.py -> _collect_concert_info()
        목적: concert_info.csv 파일 생성을 위한 콘서트 상세 정보 수집
        테이블: concert_info.csv
        컬럼: concert_title, category, content, img_url
        """
        return f"""{DataCollectionPrompts.COMMON_SOURCE_RULES}

{artist_name}의 "{concert_title}" 콘서트에 대한 **실제 확인된** 관객들이 알아야 할 실용적인 정보를 검색해주세요.

📋 수집할 정보 카테고리 (각각 별도 항목으로 작성):

2. **입장 및 관람 규칙** 
   - 콘서트장 입장 시간 및 절차
   - 금지 물품이나 반입 제한 사항
   - 사진/동영상 촬영 규칙

3. **티켓팅 및 가격 정보** (⚠️ 공식 확인된 정보만!)
   - 공식 티켓 사이트에서 확인된 정확한 가격대만
   - 공식 발표된 예매 방법만 (추측 금지)
   - 선예매, 팬클럽 예매 등은 공식 발표가 있는 경우만 포함

4. **공연장 편의시설**
   - 주차장, 교통편 안내
   - 공연장 내 편의시설 (카페, 굿즈샵 등)

🔥 문체 규칙:
- ~요, ~니다를 섞어 친근하게 작성
- 관객 입장에서 유용한 실용적 정보 위주로 작성

🚨 **극도로 중요한 검증 규칙:**
- **티켓 가격/시간/날짜는 공식 출처에서 100% 확인된 것만 포함**
- **팬클럽 선예매, 카드사 선예매 등은 공식 발표가 없으면 절대 언급 금지**
- **"일반적으로", "보통", "대개"와 같은 추측성 표현 완전 금지**
- **다른 콘서트 정보 기반 추정 절대 금지**
- **확실하지 않은 정보는 해당 카테고리 자체를 제외**

✅ **허용되는 출처:**
- 인터파크, 멜론티켓, YES24 등 공식 티켓 사이트
- 아티스트 공식 홈페이지/SNS  
- 공연장 공식 웹사이트
- 신뢰할 수 있는 언론 보도

⚠️ **최종 검증:**
각 항목 작성 전 3단계 자문:
1. 이 정보의 출처가 명확한가?
2. 100% 확실한 사실인가?
3. 추측이나 가정이 포함되어 있지 않은가?

**의심스러우면 해당 정보를 포함하지 마세요!**

반드시 JSON 배열로 응답:
[{{"concert_title": "{concert_title}", "category": "확인된카테고리명", "content": "검증된 정보만 (20자 이상)", "img_url": ""}}]"""

    @staticmethod
    def get_concert_label_introduction_prompt(artist_name: str, concert_title: str) -> str:
        """
        사용 위치: data_processing/enhanced_data_collector.py -> _collect_label_introduction()
        목적: concerts.csv의 label, introduction 컬럼 수집
        테이블: concerts.csv
        컬럼: label, introduction
        """
        return f"""{artist_name}의 "{concert_title}" 콘서트에 대한 정보를 검색해주세요.

🔥 중요 규칙:
1. label: 선택사항 (특별한 이슈가 있을 때만)
   - 현재 화제가 되는 특별한 부분이 있으면: "\(내용) 콘서트" 형식
   - 🎯 특히 주목해야 할 화제성:
     * 밴드 해체 후 재결합/재유니온 (예: "15년 만의 재결합 콘서트")
     * 오랜만의 내한 (예: "10년 만의 내한 콘서트", "데뷔 n년 만의 첫 내한 콘서트")
     * 마지막 공연/고별 투어 (예: "해체 전 마지막 투어 콘서트") 
     * 매진 임박/초고속 매진 (예: "매진 임박 콘서트", "최근 핫한 콘서트")
     * 특별한 기념 투어 (예: "데뷔 20주년 기념 콘서트")
   - 특별한 화제가 없으면: 빈 문자열

2. introduction: 필수 항목 (반드시 채워야 함)
   - {artist_name}에 대한 매력적인 소개 문구 작성 (~요, ~니다를 섞어 친근하게 작성)
   - 아티스트의 인기곡, 특징, 한국에서의 인지도 등 포함
   - 형식 자유롭게, 화제성 있게 작성
   - 예: "일본 대표 록밴드 뮤즈입니다! 세계적 히트곡 Supermassive Black Hole로 유명합니다"
   - 예: "K-POP 4세대 대표 걸그룹입니다! 글로벌 히트곡 Next Level로 전 세계 팬들이 열광합니다"

🎯 문체 규칙:
- ~요, ~니다를 섞어 자연스럽고 매력적인 문장
- 정확한 한국어 문법과 맞춤법 준수

❌ 금지사항:
- introduction에 "정보가 없다", "검색할 수 없다" 등의 문구 절대 금지
- introduction이 비어있으면 안됨 (반드시 채워야 함)

JSON 형식으로만 답변:
{{"label": "특별한 화제가 있으면 내용, 없으면 빈 문자열", "introduction": "매력적인 아티스트 소개 (~요, ~니다를 섞어 친근하게 작성, 반드시 채우기)"}}

JSON만 반환하세요."""

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
        return f"""{artist_name}의 과거 콘서트 기록을 바탕으로 "{concert_title}" 예상 셋리스트를 구성해주세요.

🎯 **1단계: 과거 셋리스트 기록 검색** (우선순위)
1. **setlist.fm** - "{artist_name}" 검색하여 최근 1-2년 내 라이브 공연 셋리스트 찾기
2. **콘서트 리뷰 사이트** - "{artist_name} live concert setlist", "{artist_name} tour setlist" 검색
3. **팬 커뮤니티** - "{artist_name}" 콘서트 후기나 셋리스트 기록
4. **유튜브** - "{artist_name} live concert full" 영상에서 셋리스트 확인

🔍 **검색 전략:**
- 최근 2년 내 라이브 공연 기록 우선 검색
- 비슷한 규모의 공연장(홀버트, 라이브하우스 등) 기록 참고
- 동일 투어나 시리즈 공연 셋리스트 분석

🎵 **2단계: 예상 셋리스트 구성 방법**

**A. 과거 기록이 있는 경우:**
1. 과거 셋리스트에서 **고정 리스트** 추출 (보통 70-80% 유지)
   - 오프닝 정법곡
   - 대표곡/히트곡 (거의 본업 곡들)
   - 앙코르 정법곡
2. **신곡/최근 곡** 추가 (20-30%)
   - 2023-2024년 신발매 앨범 수록곡
   - 최근 인기 차트 상위곡
   - SNS나 스트리밍에서 화제가 된 곡

**B. 과거 기록이 없는 경우:**
1. "{artist_name} greatest hits", "{artist_name} popular songs" 검색
2. 대표곡 20개 이상 찾아서 콘서트용 15-20곡 선별
3. 아래 구성에 맞게 배치

🎵 **셋리스트 구성** (반드시 15곡 이상):
- 1-3번: 강렬한 오프닝 히트곡 (과거 기록 바탕)
- 4-8번: 대표곡 + 신곡 섞어서 배치
- 9-12번: 중간 부 다양한 분위기 곡들
- 13-15번: 감동적인 발라드/명곡 (과거 기록 기준)
- 16번 이후: 앙코르 대표곡 (사실상 고정곡)

📝 **예상 셋리스트 작성 예시:**
"과거 {artist_name} 콘서트에서 자주 연주된 곡들을 기반으로, 최신 앨범 수록곡 2-3곡을 추가하여 구성"

⚠️ **절대 규칙:**
- artist는 반드시 "{artist_name}" 사용
- 실제 존재하는 곡 제목만 사용
- order_index는 1부터 순서대로
- 최소 15곡, 최대 25곡
- fanchant는 항상 빈 문자열("") 사용

🎤 **fanchant_point 작성 규칙 (매우 중요!):**

**✅ 포함해야 할 내용 (실제 출처가 있는 경우만):**
1. **구체적인 응원법**: "'OOO' 가사에서 관객이 'XXX' 외치기"
2. **특별한 안무나 동작**: "후렴구에서 손 흔들기", "간주 부분에서 점프하기"
3. **감상 포인트**: "고음 부분에서 소름 돋는 순간", "어쿠스틱 기타 솔로 구간"
4. **라이브만의 특징**: "이 곡에서만 하는 특별한 퍼포먼스", "팬들과 함께 부르는 구간"

**❌ 절대 금지:**
- 추측이나 일반적인 내용 ("신나는 곡", "함께 부르기" 등)
- 출처 없는 응원법 정보
- 기본적이거나 뻔한 설명

**📝 작성 기준:**
- 실제 콘서트 영상, 팬 후기, 공식 정보에서 확인된 내용만
- 구체적인 가사 부분이나 특정 구간 명시
- 30자 이내로 간결하게 작성
- **확실하지 않으면 빈 문자열("") 사용**

**예시:**
- ✅ "'We will rock you' 부분에서 박수 3번"
- ✅ "간주에서 관객석 조명 켜달라고 요청"
- ❌ "함께 부르는 구간" (너무 일반적)
- ❌ "신나게 응원하기" (추측성)

🎵 **YouTube ID 수집 규칙 (매우 중요!):**
- 각 곡마다 YouTube에서 공식 뮤직비디오나 라이브 영상 검색
- "{{artist_name}} {{song_title}}" 또는 "{{artist_name}} {{song_title}} official" 검색  
- 공식 채널이나 신뢰할 수 있는 영상의 YouTube ID만 수집
- YouTube URL에서 v= 뒤의 11자리 문자열만 추출 (예: "dQw4w9WgXcQ")
- 확실하지 않으면 빈 문자열("") 사용

🔴 **매우 중요: 모든 곡은 반드시 songs 배열에 포함되어야 합니다!**
- setlist_songs에 있는 모든 곡은 songs 배열에도 반드시 포함
- 각 곡마다 title과 artist 정보는 필수
- lyrics, pronunciation, translation이 없어도 빈 문자열("")로 포함
- youtube_id가 없어도 빈 문자열("")로 포함

JSON 응답 (배열 형태 유지):
{{"setlist_songs": [{{"setlist_title": "{concert_title}", "song_title": "실제곡제목", "setlist_date": "2025-01-01", "order_index": 1, "fanchant": "", "fanchant_point": "구체적인출처있는정보또는빈문자열"}}, ...15곡이상...], "songs": [{{"title": "실제곡제목", "artist": "{artist_name}", "lyrics": "", "pronunciation": "", "translation": "", "youtube_id": "실제YouTube영상ID"}}, ...setlist_songs의 모든 곡 포함...]}}

💬 **추가 안내:** 과거 셋리스트 기록이 있다면 그를 기반으로 하고, 없다면 대표곡 기반으로 구성해주세요."""

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
날짜: {date}

⭐ 필수 규칙: 아티스트명 통일 ⭐
- 모든 artist 필드는 반드시 "{artist_name}" 그대로 사용 (절대 변경 금지)
- 다른 표기나 영문명으로 변경하지 말 것

🔍 **상세 검색 전략:**
1. **setlist.fm** 검색:
   - 사이트에서 "{artist_name}" 직접 검색
   - "{date}" 날짜 또는 비슷한 시기 공연 셋리스트 확인
   - 동일 투어/시리즈의 다른 날짜 공연 기록 참고

2. **콘서트 리뷰 및 팬 커뮤니티:**
   - "{artist_name} {date} setlist", "{artist_name} 콘서트 후기" 검색
   - 일본/한국 팬 커뮤니티에서 실제 참관자 후기 찾기

3. **유튜브/소셜 미디어:**
   - "{artist_name} {venue} live" 영상 검색
   - 팬들이 올린 콘서트 영상에서 셋리스트 확인

4. **음악 매체 리뷰:**
   - 음악 전문 사이트의 콘서트 리뷰 기사
   - 공연 그 날의 언론 보도 확인

📋 요구사항:
- 정확한 셋리스트만 제공 (추측 금지)
- order_index는 1부터 순서대로 배정 (매우 중요!)
- 앙코르 곡도 포함하여 순서 유지
- 실제 정보가 없으면 빈 배열 반환

🎤 **fanchant_point 작성 규칙 (매우 중요!):**

**✅ 포함할 내용 (실제 출처가 있는 경우만):**
1. **구체적인 응원법**: "'특정가사' 부분에서 관객이 'XX' 외치기"
2. **특별한 동작/안무**: "후렴구에서 손 흔들기", "간주에서 점프"
3. **감상 포인트**: "고음 부분 소름 순간", "어쿠스틱 솔로 구간"
4. **라이브 특징**: "이 곡만의 특별 퍼포먼스", "팬과 함께 부르는 구간"

**❌ 절대 금지:**
- 추측이나 일반적 내용 ("신나는 곡", "함께 부르기")
- 출처 없는 정보
- 기본적인 설명

**📝 작성 기준:**
- 실제 영상/후기에서 확인된 내용만
- 구체적인 가사나 구간 명시 (30자 이내)
- **확실하지 않으면 반드시 빈 문자열("") 사용**

**예시:**
- ✅ "'We will rock you'에서 박수 3번"
- ✅ "기타 솔로 구간에서 관객 환호"
- ❌ "함께 부르는 부분" (일반적)
- ❌ "신나게 응원" (추측)

🎵 **YouTube ID 수집 규칙 (매우 중요!):**
- 각 곡마다 YouTube에서 공식 뮤직비디오나 라이브 영상 검색
- "{{artist_name}} {{song_title}}" 또는 "{{artist_name}} {{song_title}} official" 검색
- 공식 채널이나 신뢰할 수 있는 영상의 YouTube ID만 수집
- YouTube URL에서 v= 뒤의 11자리 문자열만 추출 (예: "dQw4w9WgXcQ")
- 확실하지 않으면 빈 문자열("") 사용

🔴 **매우 중요: 모든 곡은 반드시 songs 배열에 포함되어야 합니다!**
- setlist_songs에 있는 모든 곡은 songs 배열에도 반드시 포함
- 각 곡마다 title과 artist 정보는 필수
- lyrics, pronunciation, translation이 없어도 빈 문자열("")로 포함
- youtube_id가 없어도 빈 문자열("")로 포함

JSON 형식으로만 응답:
{{"setlist_songs": [{{"setlist_title": "{concert_title}", "song_title": "정확한실제곡제목1", "setlist_date": "{date}", "order_index": 1, "fanchant": "", "fanchant_point": "구체적출처있는정보또는빈문자열"}}, {{"setlist_title": "{concert_title}", "song_title": "정확한실제곡제목2", "setlist_date": "{date}", "order_index": 2, "fanchant": "", "fanchant_point": ""}}, ...], "songs": [{{"title": "정확한실제곡제목1", "artist": "{artist_name}", "lyrics": "", "pronunciation": "", "translation": "", "youtube_id": "실제YouTube영상ID"}}, {{"title": "정확한실제곡제목2", "artist": "{artist_name}", "lyrics": "", "pronunciation": "", "translation": "", "youtube_id": "실제YouTube영상ID"}}, ...setlist_songs의 모든 곡 포함...]}}

⚠️ **중요:**
- order_index 순서 정확히 지키기!
- fanchant_point는 확실한 출처가 있는 정보만 (추측 금지)
- 애매하면 빈 문자열("") 사용하기
- 실제 정보만 사용!"""

    # =========================================================================
    # CULTURES 테이블 관련 프롬프트
    # =========================================================================
    
    @staticmethod
    def get_culture_info_prompt(artist_name: str, concert_title: str, concert) -> str:
        """
        사용 위치: data_processing/enhanced_data_collector.py -> _collect_culture_info()
        목적: cultures.csv의 문화 정보 수집
        테이블: cultures.csv
        컬럼: 모든 컬럼
        """
        status_korean = {
            'UPCOMING': '예정된',
            'ONGOING': '진행 중인', 
            'PAST': '완료된'
        }.get(concert.status, concert.status)
        
        return f"""{DataCollectionPrompts.COMMON_SOURCE_RULES}

🚨 **중요: 정확한 콘서트 정보를 반드시 확인하세요!**

**검색 대상 콘서트:**
- 아티스트: {artist_name}
- 콘서트명: "{concert_title}"
- 공연장: {concert.venue}
- 날짜: {concert.start_date}
- 상태: {status_korean} 공연

⚠️ **절대 주의사항:**
- 다른 공연장에서 열린 과거 공연과 혼동하지 마세요
- {status_korean} 공연이므로 과거형/현재형 구분 정확히
- {concert.venue}에서 열리는 이 특정 공연에 대한 정보만 수집

{artist_name}의 "{concert_title}" ({concert.venue}, {concert.start_date}) 콘서트만의 **실제 확인된** 독특하고 고유한 문화적 특징을 검색해주세요.

⭐ 필수 규칙: 아티스트명 통일 ⭐
- artist_name 필드는 반드시 "{artist_name}" 그대로 사용
- 다른 표기나 영문명 사용 금지
- 모든 JSON 응답에서 동일하게 "{artist_name}" 사용

찾아야 할 문화 정보 (2가지 출처 허용):

**A. 이 특정 콘서트 관련 정보 (최우선):**
1. {concert.venue}에서의 이 특정 공연 팬덤 문화
2. 이 콘서트만의 특별한 전통이나 응원법  
3. 해당 공연장의 실용적 정보 (좌석, 시야, 교통, 주차 등)

**B. {artist_name}의 일반적인 콘서트 문화 패턴 (보완 정보):**
4. 아티스트의 다른 콘서트에서 공통적으로 나타나는 팬덤 문화
5. {artist_name} 콘서트에서 자주 보이는 응원 방식과 관객 참여 패턴  
6. 아티스트와 팬들 간의 일반적인 소통 방식
7. {artist_name} 콘서트 관람 시 알아두면 좋은 문화적 특징

⚠️ **정보 출처 구분:**
- A 유형: "{concert_title} {concert.venue}" 등으로 명확히 표기
- B 유형: "{artist_name} 콘서트" 또는 "다른 공연에서도" 등으로 표기

📝 문체 규칙:
- ~요, ~니다를 섞어 친근하게 작성
- 정확한 한국어 문법과 맞춤법 준수

🎨 이미지 URL 수집 규칙:
- 각 문화 정보에 어울리는 시각적 이미지 필수 제공
- 콘서트장 사진, 팬들 응원 모습, 아티스트 공연 장면 등
- 저작권 없는 이미지나 공식 사진 우선 (Unsplash, Pixabay, 공식 SNS 등)
- Pinterest나 일반 웹사이트 이미지도 가능
- 해당 문화의 느낌을 잘 표현하는 분위기 있는 사진

🔥 **최종 검증 필수:**
1. 이 정보가 정말 {concert.venue}에서 열리는 {concert.start_date} 공연에 대한 것인가?
2. 다른 공연장이나 다른 날짜 공연과 혼동하지 않았는가?
3. {status_korean} 공연이므로 시제(과거/현재/미래)가 정확한가?

**확실하지 않은 정보는 포함하지 마세요!**

JSON 배열 형식으로 반환:
[{{"artist_name": "{artist_name}", "concert_title": "{concert_title}", "title": "문화제목", "content": "구체적인 문화 내용 ({concert.venue}, {concert.start_date} 공연 관련)", "img_url": "관련이미지URL필수"}}]

⚠️ 중요: img_url은 반드시 채워야 하며, 빈 문자열 금지"""

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
        return f"""{DataCollectionPrompts.COMMON_SOURCE_RULES}

🚨 **스케줄 정보 수집 - 극도로 엄격한 검증 필요** 🚨

"{artist_name}"의 "{concert_title}" 콘서트 관련 **실제 존재하는 확인된 일정만** 수집하세요.

🔍 **허용되는 검색 출처 (이것만 사용!):**
1. **인터파크 티켓** - interpark.com
2. **멜론티켓** - ticket.melon.com  
3. **YES24 티켓** - ticket.yes24.com
4. **아티스트 공식 웹사이트/SNS**
5. **주최사 공식 발표 페이지**
6. **네이버 뉴스/조선일보/중앙일보 등 언론사 기사**

🚫 **절대 금지 - 즉시 중단하세요:**
- "일반적으로 ~시에 시작한다"
- "보통 콘서트는 ~시간"  
- "대개 입장은 ~시간 전"
- "아마도", "추정", "예상", "보통"
- 블로그, 카페, 개인 후기의 추측
- 다른 콘서트나 비슷한 공연 시간 기반 추정
- 티켓 사이트 없이 시간만 찾은 정보

✅ **수집 가능한 정보 (100% 확실한 경우만!):**
- 공식 티켓 사이트에 명시된 정확한 공연 시작 시간
- 공식 발표된 티켓 예매 오픈 일시
- 공식 SNS에서 발표한 부대 행사 일정
- 언론 보도에 인용된 주최사 공식 발표 시간

⚠️ **3단계 검증 필수:**
1. **출처 확인**: 위의 허용 출처에서 나온 정보인가?
2. **구체성 검증**: 정확한 날짜와 시간이 명시되어 있는가?
3. **공식성 확인**: 추측이 아닌 공식 발표인가?

📝 **응답 형식:**
- **정보를 찾은 경우**: [{{"concert_title": "{concert_title}", "category": "구체적카테고리", "scheduled_at": "정확한날짜시간"}}]
- **정보를 찾지 못한 경우**: []

🔥 **최종 경고:**
**99% 확실하지 않으면 빈 배열 [] 반환하세요!**
**틀린 정보는 사용자에게 피해를 줍니다!**
**의심스러우면 절대 포함하지 마세요!**"""

    # =========================================================================
    # CONCERT_GENRES 테이블 관련 프롬프트
    # =========================================================================
    
    @staticmethod
    def get_ticket_info_prompt(artist_name: str, concert_title: str) -> str:
        """
        사용 위치: data_processing/enhanced_data_collector.py -> _search_ticket_info()
        목적: 티켓 예매 정보 검색
        """
        return f""""{artist_name}"의 "{concert_title}" 콘서트 티켓 예매 정보를 찾아주세요.

요청 사항:
1. 예매 시작 시간
2. 티켓 가격
3. 예매 사이트 URL  
4. 티켓 오픈 일정

모든 정보는 공식 출처에서 확인된 것만 포함하세요.
찾을 수 없는 정보는 빈 값으로 반환하세요."""

    @staticmethod
    def get_concert_genre_fallback_prompt(concert_title: str) -> str:
        """
        사용 위치: data_processing/enhanced_data_collector.py -> _collect_concert_genres()
        목적: 장르 정보 fallback 프롬프트
        """
        return f""""{concert_title}" 콘서트의 장르를 아래 6개 중에서 1개만 선택해주세요.

1. JPOP - 일본 음악
2. RAP_HIPHOP - 랩/힙합
3. ROCK_METAL - 록/메탈
4. ACOUSTIC - 어쿠스틱
5. CLASSIC_JAZZ - 클래식/재즈
6. ELECTRONIC - 일렉트로닉

JSON 배열로 응답: [{{"genre_id": 숫자, "name": "장르명"}}]"""

    @staticmethod
    def get_concert_genre_prompt(artist_name: str, concert_title: str) -> str:
        """
        사용 위치: data_processing/enhanced_data_collector.py -> _collect_concert_genres()
        목적: concert_genres.csv의 장르 매칭 정보 수집
        테이블: concert_genres.csv
        컬럼: 모든 컬럼
        """
        return f"""{DataCollectionPrompts.COMMON_SOURCE_RULES}

"{artist_name}"를 **공식 출처에서** 검색해서 음악 장르를 확인하고, 아래 6개 중에서 가장 적합한 장르를 선택해주세요.

🎯 장르 선택 기준:
1. 구글에서 "{artist_name} music genre", "{artist_name} 음악 장르" 검색
2. 위키피디아, 음악 사이트 등에서 공식 장르 확인
3. 아래 6개 중 가장 가까운 장르 선택

📋 선택 가능한 장르 (반드시 이 중에서만 선택):
1. JPOP (id: 1) - 일본 아티스트, J-POP, J-ROCK
2. RAP_HIPHOP (id: 2) - 랩, 힙합, R&B, 트랩
3. ROCK_METAL (id: 3) - 록, 메탈, 하드록, 얼터너티브
4. ACOUSTIC (id: 4) - 어쿠스틱, 포크, 인디, 싱어송라이터
5. CLASSIC_JAZZ (id: 5) - 클래식, 재즈, 블루스
6. ELECTRONIC (id: 6) - 일렉트로닉, EDM, 하우스, 테크노

🎵 장르 매칭 예시:
- Oasis → ROCK_METAL (브릿팝/록 밴드)
- BTS → RAP_HIPHOP (K-POP이지만 힙합 요소 강함)
- IU → ACOUSTIC (발라드/어쿠스틱 중심)
- Perfume → ELECTRONIC (일렉트로닉 팝)
- ONE OK ROCK → ROCK_METAL (일본 록밴드)

⚠️ 중요:
- 메인 장르 1개만 선택 (가장 대표적인 것)
- K-POP은 음악 스타일에 따라 RAP_HIPHOP 또는 ACOUSTIC 선택
- concert_id와 concert_title은 "{concert_title}" 사용

JSON 배열로 응답:
[{{"concert_id": "{concert_title}", "concert_title": "{concert_title}", "genre_id": 숫자, "name": "장르명"}}]"""

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
        return f"""{DataCollectionPrompts.COMMON_SOURCE_RULES}

"{artist_name}"의 "{concert_title}" 콘서트의 **실제 확인된 굿즈 정보만** 검색해주세요.

🔍 **공식 출처에서만 확인:**
1. **공식 굿즈 판매 사이트**
   - "{artist_name} 공식 굿즈", "{concert_title} 굿즈" 검색
   - 공식 온라인 스토어, 공식 티켓 사이트의 굿즈 섹션

2. **공식 SNS 발표**
   - "{artist_name}" 공식 트위터, 인스타그램
   - 주최사 공식 채널의 굿즈 안내

3. **신뢰할 수 있는 언론 보도**
   - 음악 매체의 콘서트 굿즈 소개 기사
   - 공식 보도자료

🚫 **절대 포함하지 말 것:**
- 추측으로 만든 "일반적인" 굿즈 목록
- 다른 콘서트의 굿즈 정보 기반 유추
- "보통 ~이 있을 것" 같은 가정
- 확인되지 않은 가격 정보

✅ **포함 가능한 정보 (확실한 출처가 있는 경우만):**
- 공식 발표된 굿즈 이름과 가격
- 공식적으로 공개된 굿즈 이미지
- 공식 발표된 판매 상태

📝 **JSON 형식으로만 응답:**
[
  {{"concert_title": "{concert_title}", "name": "확인된굿즈명", "price": "확인된가격", "img_url": "공식이미지URL"}}
]

**🚨 중요: 굿즈 정보를 찾을 수 없으면 빈 배열 [] 반환하세요!**
**잘못된 정보보다 빈 값이 훨씬 낫습니다!**"""


class LyricsPrompts:
    """가사 번역/발음 관련 프롬프트"""
    
    @staticmethod
    def get_translation_prompt(lyrics: str, song_title: str = "", artist: str = "") -> str:
        """
        사용 위치: src/lyrics_translator.py -> translate_lyrics()
        목적: 영어 가사를 한국어로 번역
        """
        return f"""다음 영어 가사를 한국어로 번역해주세요. 오직 번역된 가사만 출력하세요.

{lyrics}

중요: 다른 설명이나 추가 정보 없이 번역된 가사만 출력하세요."""

    @staticmethod
    def get_pronunciation_prompt(lyrics: str, song_title: str = "", artist: str = "") -> str:
        """
        사용 위치: src/lyrics_translator.py -> convert_to_pronunciation()
        목적: 영어 가사를 한국어 발음으로 변환
        """
        return f"""다음 영어 가사를 한국어 발음으로 변환해주세요. 오직 한국어 발음만 출력하세요.

{lyrics}

중요: 다른 설명이나 추가 정보 없이 한국어 발음만 출력하세요."""

    @staticmethod
    def get_combined_translation_prompt(lyrics: str) -> str:
        """
        사용 위치: scripts/add_translation_to_songs.py
        목적: 영어 가사의 한국어 발음과 해석을 동시에 생성
        """
        return f"""다음 영어 가사에 대해 한국어 발음과 한국어 해석을 생성해주세요.

영어 가사:
{lyrics}

출력 형식 (반드시 이 형식을 지켜주세요):
### 한국어 발음:
(여기에 한국어 발음)

### 한국어 해석:
(여기에 한국어 번역)

중요: 위 형식을 정확히 지켜주세요. 각 섹션 제목은 "### 한국어 발음:"과 "### 한국어 해석:"으로 시작해야 합니다."""


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
        return """당신은 한국 내한 콘서트 정보를 수집하는 전문가입니다.

핵심 원칙:
1. 최신 정보만 제공 (추측 금지)
2. 정확하지 않은 정보는 제공하지 않음
3. 요청된 형식(JSON 등) 정확히 준수
4. "구글 검색을 통해" 같은 불필요한 설명 절대 금지
5. 사실만 전달, 설명 없이 답변

검색 우선순위:
- 공식 발표 > 공식 티켓 사이트 > 신뢰할 수 있는 언론사 > 기타 소스

❌ 절대 금지 표현:
- "구글 검색을 통해 찾아드렸습니다"
- "검색 결과에 따르면"
- "정보를 찾을 수 없습니다" (비어있을 때 예외)
- 기타 검색 과정에 대한 언급

✅ 올바른 응답: 매우 간결하게 핵심 내용만 전달"""

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
    def get_perplexity_system_message() -> str:
        """
        사용 위치: src/perplexity_api.py -> query_with_search()
        목적: Perplexity API 시스템 메시지
        """
        return """당신은 웹 검색을 통해서만 정보를 제공하는 연구 어시스턴트입니다. 
        웹 검색 결과에서 명시적으로 찾은 정보만을 사용해야 하며, 절대로 추측이나 추론을 하지 마세요.
        특정 정보를 웹 검색으로 찾을 수 없다면 추측하지 말고 빈 정보를 반환하세요. 
        한국의 콘서트 장소, 티켓팅 사이트, 팬 커뮤니티에서 정보를 찾는 것에 집중하세요.
        모든 응답은 반드시 한국어로 작성해야 합니다."""
    
    @staticmethod
    def get_perplexity_enhanced_prompt(prompt: str) -> str:
        """
        사용 위치: src/perplexity_api.py -> query_with_search()
        목적: Perplexity API 검색 강화 프롬프트
        """
        return f"""중요: 웹 검색을 통해 찾은 정보만 제공해야 합니다. 추측이나 추론은 절대 하지 마세요.

{prompt}

검색 요구사항:
1. 웹 검색 결과에서 명시적으로 찾은 정보만 사용
2. 공식 발표, 티켓 판매, 공연장 정보를 찾으세요
3. 검색 결과에서 정보를 찾을 수 없으면 추측하지 말고 빈 값을 반환하세요
4. 항상 구체적인 출처와 날짜를 검색 결과에서 포함하세요
5. 최근 3년 내 정보를 우선하세요
6. 모든 응답은 한국어로 작성하세요"""


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
