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
│  └─ 모든 컬럼    │ get_artist_basic_info_prompt()                          │
├─────────────────┼─────────────────────────────────────────────────────────┤
│ concerts.csv    │                                                         │
│  ├─ artist      │ get_artist_name_prompt()                                │
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
- lib/data_collector.py (메인 데이터 수집)
- core/apis/gemini_api.py (Gemini API 시스템 프롬프트)
- core/apis/perplexity_api.py (Perplexity API 시스템 프롬프트)
"""
from typing import Optional

CONCERT_KEYWORDS = [
    "콘서트", "concert", "투어", "tour", "공연", "라이브", "live",
    "단독", "showcase", "쇼케이스", "페스티벌", "festival",
    "티켓", "ticket", "예매", "공연일", "내한",
    "추가공연", "앙코르", "encore", "추가 티켓", "추가 오픈", "2차", "3차",
]


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
    def get_artist_basic_info_prompt(
        artist_name: str, 
        concert_title: Optional[str] = None, 
        musicbrainz_context: Optional[str] = None,
        song_examples: Optional[list] = None
    ) -> str:
        json_example = '''{
    "artist": "Oasis (오아시스)",
    "category": "록, 브릿팝 아티스트",
    "detail": "1991년 결성된 영국의 5인조 브릿팝 밴드입니다. 보컬 리암 갤러거, 기타 노엘 갤러거, 베이스 폴 맥기건, 드럼 토니 맥캐롤, 리듬기타 폴 아서스로 구성되어 있습니다. 90년대 브릿팝 부흥을 이끌며 전설적인 밴드로 자리잡았습니다. 대표곡으로는 'Wonderwall', 'Don't Look Back in Anger' 등이 있습니다.",
    "instagram_url": "https://www.instagram.com/oasis/",
    "keywords": "브릿팝, 록, 밴드, 90년대",
    "img_url": "",
    "debut_date": "1991",
    "nationality": "영국",
    "group_type": "밴드"
    }'''

        concert_context = f"공연명 '{concert_title}'에 출연하는 " if concert_title else ""

        # ✅ 대표곡 관련 지시문 개선
        if song_examples and len(song_examples) > 0:
            song_instruction = f"""
    ✅ 대표곡 (이 곡들을 detail 마지막 문장에 포함):
    {', '.join(f"'{song}'" for song in song_examples[:3])}
    """
        else:
            song_instruction = f"""
🔍 **대표곡 작성 규칙 (매우 중요!):**
1. '{artist_name}' + 'popular songs' 또는 '{artist_name}' + '대표곡'으로 웹 검색
2. Spotify, Apple Music, YouTube에서 실제 조회수/재생수 높은 곡 확인
3. **검증 단계 (필수):**
   - 곡명 길이가 1자 이상인가?
   - 곡명이 실제로 존재하는가?
   - 위 조건 중 하나라도 NO면 → 대표곡 문장 자체를 작성하지 마세요

✅ **대표곡이 있을 때만:**
   - 일본어: "대표곡으로는 '名前を呼ぶよ', '風が吹く街' 등이 있습니다."
   - 영어: "대표곡으로는 'Wonderwall', 'Live Forever' 등이 있습니다."
   - 러시아어: "대표곡으로는 'Горгород', 'Fata Morgana' 등이 있습니다."

✅ **대표곡을 모를 때:**
   - "다양한 애니메이션 음악 작업에 참여했습니다."
   - "국내외에서 큰 인정을 받았습니다."
   - "활발한 라이브 활동을 이어가고 있습니다."

🚫 **절대 금지 - 이렇게 하면 안 됨:**
   - "대표곡으로는 '', '' 등" ❌
   - "2016년 싱글 ''로 데뷔" ❌
   - "밴드명을 ''로 변경" ❌
   - **대표곡 모르면 그 문장 자체를 삭제하세요!**

"""

        return f"""
    {concert_context}아티스트 '{artist_name}'의 정보를 JSON으로 생성하세요.

    ⚠️ **매우 중요: 반드시 웹 검색으로 정확한 정보만 작성!**
    {song_instruction}

필수 9개 필드:
- artist: "영문명 (한국어)" 형식
- category: 직업
- detail: 3~4문장 (아래 형식 참고)
- instagram_url: 공식 계정 (없으면 "")
- keywords: 음악 장르
- img_url: 프로필 이미지 (없으면 "")
- debut_date: 데뷔 연도 YYYY (솔로는 첫 음반/싱글 발매 연도, 그룹은 결성 연도, 생년월일 아님, 못 찾으면 "")
- nationality: 국적
- group_type: 솔로/그룹/밴드/듀오

⚠️ detail 작성 규칙:
- 찾은 정보만 포함, 못 찾은 문장은 생략
- 순서: 결성/데뷔 → 멤버구성(그룹만) → 활동내용


🚫 **절대 금지사항**:
- 인용 출처 번호 [1], [2, 3] 같은 것 포함 금지
- 빈 괄호 "()", 빈 따옴표 "" 포함 금지
- 추측으로 멤버 이름생성 금지

🔴 **최종 검증 (작성 완료 후):**
- detail에 빈 따옴표('', "", '  ')가 하나라도 있으면 그 문장을 통째로 삭제
- 대표곡을 확실히 못 찾았으면 "대표곡으로는" 문장 자체를 아예 작성하지 말 것

**응답은 순수 JSON만 반환하세요. 설명, 주석, 인용 번호 모두 금지!**
```json
    {json_example}
```
"""
    @staticmethod
    def get_artist_songs_prompt(artist_name: str, concert_title: Optional[str] = None) -> str:
        """
        사용 위치: lib/data_collector.py -> _collect_artist_basic_info()
        목적: 아티스트의 대표곡 검색
        """
        concert_context = f"공연명 '{concert_title}'에 출연하는 " if concert_title else ""
        return f"""{concert_context}아티스트 '{artist_name}'의 인기곡/대표곡 1~2개를 검색해서 JSON으로 반환하세요.

        ⚠️ 규칙:
        - Spotify, Apple Music, YouTube 등에서 실제 인기곡 확인
        - 곡명은 원어 그대로 (일본어면 일본어: '幽霊' O, 'Yurei' X)
        - 빈 문자열 "" 금지! 못 찾으면 배열 자체를 비워서 반환

        형식: {{"songs": ["곡1", "곡2"]}}
        못 찾으면: {{"songs": []}}
"""
    # =========================================================================
    # CONCERTS 테이블 관련 프롬프트
    # =========================================================================
    
    @staticmethod
    def get_artist_name_prompt(concert_title: str) -> str:
        """
        사용 위치: lib/data_collector.py -> _extract_artist_from_title()
        목적: 콘서트 제목에서 아티스트 공식 활동명(raw)만 추출 - 형식 변환 없음
        """
        return f""""{concert_title} 아티스트 활동명" 으로 검색해서 이 공연의 메인 아티스트/밴드 공식 활동명을 찾아줘.

규칙:
- 공식 활동명을 찾은 그대로 반환해. 언어 변환(영문/한국어 변환)은 하지 마.
- 약어가 있으면 공식 정식 활동명으로 찾아서 반환해.
- 이벤트/시리즈명인지 아티스트명인지 불분명하면 검색으로 확인해.
- 콜라보/합동 공연(ArtistA x ArtistB 등)이면 첫 번째 아티스트만 반환해.
- "x", "&", "ft.", "vs" 등 구분자 자체는 반환하지 마.
- 찾을 수 없으면 빈 문자열 반환.

반드시 JSON으로 응답:
{{"artist": "공식 활동명"}}"""

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

1. **입장 및 관람 규칙**
   - 콘서트장 입장 시간 및 절차
   - 금지 물품이나 반입 제한 사항
   - 사진/동영상 촬영 규칙

2. **티켓팅 및 가격 정보** (⚠️ 공식 확인된 정보만!)
   - 공식 티켓 사이트에서 확인된 정확한 가격대만
   - 공식 발표된 예매 방법만 (추측 금지)
   - 선예매, 팬클럽 예매 등은 공식 발표가 있는 경우만 포함

3. **공연장 편의시설**
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
    def get_short_introduction_prompt(title: str, artist: str) -> str:
        """
    사용 위치: lib/data_collector.py -> _collect_short_introduction()
    목적: 콘서트의 한 줄 소개 문구를 생성합니다.
    테이블: concerts.csv
    컬럼: introduction
    """
        return f"""'{title}' ({artist}) 콘서트의 한 줄 소개 문구를 생성해줘.

📝 **작성 우선순위 (순서대로 시도):**

1. **내한 정보** - 첫 내한, n년 만의 내한, 재방문
2. **특별한 이벤트** - 재결합, 해체 전 마지막, 기념 투어
3. **투어/앨범** - 새 앨범 발매 기념, 투어명
4. **대표곡** - 100% 확실하게 찾은 곡만 사용
5. **장르/업적** - 장르에서의 위상, 수상 경력, 유명 프로젝트

🔍 **대표곡 검색 규칙:**
- '{artist}' + 'popular songs' 또는 '{artist}' + '대표곡'으로 웹 검색
- Spotify, YouTube에서 실제 조회수 높은 곡 확인
- **100% 확실한 곡만 사용, 조금이라도 의심되면 대표곡 사용 금지**

✅ **좋은 예시 (대표곡 있을 때):**
- "'Wonderwall', 'Don't Look Back in Anger'의 주인공 Oasis, 15년 만의 재결합 투어!"
- "히트곡 'Sprinter', 'Doja'의 주인공 Central Cee, 첫 단독 내한!"

✅ **좋은 예시 (대표곡 없을 때 - 다른 포인트 사용):**
- "일본 록 씬을 대표하는 밴드 ZUTOMAYO, 2년 만의 내한 추가공연 확정!"
- "러시아 힙합의 아이콘 Oxxxymiron, 6년 만의 월드 투어로 서울 방문!"
- "애니메이션 '문호 스트레이독스' OST로 유명한 Luck Life, 팬클럽 첫 해외 투어!"
- "J-Rock의 전설 DYGL, 9년 만의 내한으로 서울 팬들과 만난다!"
- "한국대중음악상 수상 밴드 Madmans Esprit, 3밴드 합동 투어 'GLADIUS'!"
- "YOASOBI의 보컬 ikura의 솔로 프로젝트 Lilas, 새 앨범 'Laugh' 발매 기념 첫 서울 공연!"

🚫 **절대 금지:**
- 빈 따옴표 '', '  ' 사용 금지
- "히트곡 의 주인공" 같은 어색한 문장 금지
- 대표곡을 모르면 대표곡 언급 자체를 하지 말 것
- 추측으로 곡명 생성 금지

🔴 **최종 검증:**
1. 작성 완료 후 빈 따옴표가 있으면 → 대표곡 부분 삭제하고 다른 포인트로 다시 작성
2. "히트곡 의", ", 등 대표곡" 같은 어색한 표현이 있으면 → 문장 전체 다시 작성

결과는 반드시 다음 JSON 형식이어야 해:
{{"summary": "여기에 한 줄 소개 문구"}}
"""

    @staticmethod
    def get_additional_info_prompt(title: str, artist: str) -> str:
        """
        사용 위치: lib/data_collector.py -> _collect_additional_info()
        목적: 콘서트의 추가 정보(라벨)를 수집합니다.
        테이블: concerts.csv
        컬럼: label
        """
        return f"""'{title}' ({artist}) 콘서트의 라벨 정보를 JSON으로 반환하세요.

라벨: 현재 화제가 되는 특별한 부분이 있을 때만 작성
- 🎯 특히 주목해야 할 화제성:
  * 밴드 해체 후 재결합/재유니온 (예: "15년 만의 재결합 콘서트")
  * 오랜만의 내한 (예: "10년 만의 내한 콘서트", "데뷔 n년 만의 첫 내한 콘서트")
  * 마지막 공연/고별 투어 (예: "해체 전 마지막 투어 콘서트")
  * 매진 임박/초고속 매진 (예: "매진 임박 콘서트")
  * 특별한 기념 투어 (예: "데뷔 20주년 기념 콘서트")
- 특별한 화제가 없으면 빈 문자열

JSON 형식: {{"label": "화제성 문구 또는 빈 문자열"}}"""

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
   - 최근 1~2년 내 신발매 앨범 수록곡
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
    def get_concert_genre_prompt(artist_name: str, concert_title: str) -> str:
        return f"""{DataCollectionPrompts.COMMON_SOURCE_RULES}

"{artist_name}"의 음악 장르를 공식 출처에서 확인하고, 아래 5개 중 **가장 대표적인 장르 1개**를 선택해주세요.

🔍 검색 방법:
1. "{artist_name} genre" 또는 "{artist_name} 장르" 검색
2. Wikipedia, AllMusic, MelOn, Spotify 등 공식 출처 확인
3. 아티스트의 **메인 활동 장르** 기준으로 선택 (콜라보/피처링 곡 제외)

📋 장르 정의:
1. JPOP (id: 1)
   - 일본 국적 아티스트의 J-POP, 시티팝, 아니송, J-ROCK
   - 예시: Perfume, Ado, Yoasobi, Kenshi Yonezu, RADWIMPS, amazarashi
   - ⚠️ 일본 국적이 아닌 아티스트는 절대 JPOP 선택 금지

2. ROCK_METAL (id: 2)
   - 록, 메탈, 하드록, 펑크록, 브릿팝, 얼터너티브 록, 포스트록
   - 예시: Metallica, Linkin Park, Coldplay, Oasis, My Chemical Romance
   - ⚠️ R&B, 소울, 팝 아티스트는 해당 없음

3. RAP_HIPHOP (id: 3)
   - 랩, 힙합, 트랩
   - 예시: Drake, Kendrick Lamar, Jay-Z, Travis Scott, J.I.D
   - ⚠️ R&B/소울/네오소울은 랩 비중이 높을 때만 RAP_HIPHOP, 아니면 POP

4. INDIE (id: 4)
   - 인디팝, 인디록, 포크, 싱어송라이터, 어쿠스틱
   - 예시: Novo Amor, Phoebe Bridgers, Hozier, Tommy Emmanuel

5. POP (id: 5)전
   - 메인스트림 팝, K-POP, 댄스팝, 일렉트로팝, EDM, R&B, 소울, 네오소울
   - 예시: BTS, BLACKPINK, Taylor Swift, Ed Sheeran, Dua Lipa, Giveon, Frank Ocean, The Weeknd
   - ⚠️ K-POP은 무조건 POP 포함

⚠️ 장르 선택 기준 (중요):
- **장르 경계가 명확하면 1개, 두 장르를 넘나들면 2개 선택**
- 아래 경우에만 2개 선택 허용:
  - 일본 국적 아티스트 + 록/메탈 성향 → JPOP + ROCK_METAL (예: ONE OK ROCK, NEMOPHILA, King Gnu)
  - 일본 국적 아티스트 + 인디/포크 성향 → JPOP + INDIE (예: toconoma, DEPAPEKO, Vaundy)
  - 일본 국적 아티스트 + 랩/힙합 성향 → JPOP + RAP_HIPHOP (예: Creepy Nuts, MAN WITH A MISSION)
  - 그 외 일본 국적 아티스트 → JPOP 1개만
  - 인디 성향이 주이지만 팝 차트에도 진입 → INDIE + POP (예: Sigrid, Tom Grennan, Laufey)
  - 록/메탈이 주이지만 팝 성향 곡도 많음 → ROCK_METAL + POP (예: OneRepublic, Coldplay, CHRISTOPHER)
  - 록과 랩을 번갈아 하는 경우 → ROCK_METAL + RAP_HIPHOP (예: Linkin Park, JAKE MILLER)
  - 랩이 주이지만 R&B 성향이 강한 경우 → RAP_HIPHOP + POP (예: The Weeknd, Teddy Swims)
- **위 경우 외에는 반드시 1개만 선택**
- 3개 이상 선택 절대 금지

⚠️ 자주 틀리는 케이스:
- R&B/소울/네오소울 아티스트 → 랩 비중이 높으면 RAP_HIPHOP + POP, 팝 비중이 높으면 POP
- 일본 록/메탈 밴드 → ROCK_METAL만 ❌, JPOP + ROCK_METAL ✅
- 재즈/재즈팝 아티스트 → 아래 기준으로 판단:
  - 인디/포크 성향 재즈 → INDIE (예: Laufey)
  - 힙합 크로스오버 재즈 → RAP_HIPHOP (예: Robert Glasper)
  - 팝 성향 재즈 → POP
- 비일본 아티스트 → JPOP ❌

⚠️ 매우 중요:
- 반드시 최소 1개 이상의 장르를 선택해야 합니다
- 절대 빈 배열 [] 로 응답하지 마세요

JSON 배열로만 응답 (선택한 장르 수만큼 객체 반환, 다른 텍스트 없이):
[
  {{"genre_id": 숫자, "name": "장르명"}},
  {{"genre_id": 숫자, "name": "장르명"}}
]"""
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

    @staticmethod
    def get_instagram_parse_prompt(account: str, caption: str, post_url: str) -> str:
        """Instagram 게시물에서 공연 정보 필터링 + 파싱 (1회 Gemini 호출)"""
        from datetime import datetime
        current_year = datetime.now().year
        return f"""다음은 Instagram 계정 @{account}의 게시물입니다.
게시물 URL: {post_url}

캡션:
---
{caption[:2000]}
---

이 게시물에 **외국 아티스트의 한국 내한공연** 정보가 포함되어 있는지 아래 순서로 판단하세요.

**판단 순서:**
1. 캡션에 "내한공연" 또는 "내한 공연"이 명시되고, 구체적인 공연 날짜(년/월/일)가 함께 있으면 → is_concert_post: true (아티스트 국적 무관, 아티스트명이 한국어로 표기되어 있어도 일본어·중국어 이름의 한국어 표기일 수 있음)
2. 위 조건이 아닌 경우, 아래 false 기준으로 판단

공식 공연 공지가 아니더라도 인터뷰, 영상, 홍보글 등에 내한공연 정보(아티스트명, 내한 언급, 날짜 등)가 있으면 is_concert_post: true로 설정하세요.

아래 경우는 반드시 is_concert_post: false로 설정하세요:
- 한국 아티스트(K-POP, 한국 밴드 등)의 공연
- 내한공연이 아닌 해외에서 열리는 공연
- 여러 아티스트가 출연하는 페스티벌/축제 (단독 공연이 아닌 경우)
- 내한공연 정보가 전혀 없는 순수 홍보/이벤트 게시물
- CGV, 롯데시네마, 메가박스 등 영화관에서 진행되는 라이브 뷰잉/상영 이벤트
- "온 스크린", "on screen", "스크린 상영", "극장 상영" 등 영화관 상영 이벤트
- 노래방 수록/출시 관련 게시물

규칙:
- 명확히 언급된 정보만 추출하고, 불확실하면 빈 문자열("")로 남기세요
- 날짜 형식: YYYYMMDD (예: 20250910), 연도 없으면 {current_year}년 기준
- 시간 형식: HH:MM (예: 14:00), 명시되지 않으면 빈 문자열
- artist_name: 공연하는 아티스트/밴드명 (주최사 아님), 캡션에 표기된 그대로 추출 (한국어면 한국어, 영문이면 영문)
- ticket_site: 아래 값 중 하나만 사용, 불명확하면 빈 문자열 ("NOL 티켓" / "예스24" / "멜론티켓" / "티켓링크" / "네이버 예약")
- concert_time: 공연 시작 시간 (HH:MM), 캡션에 명시된 경우만, 없으면 빈 문자열

반드시 JSON만 출력하세요:
{{
  "is_concert_post": true 또는 false,
  "artist_name": "",
  "title": "",
  "start_date": "",
  "end_date": "",
  "concert_time": "",
  "venue": "",
  "ticket_site": "",
  "ticket_url": "",
  "pre_ticketing_date": "",
  "pre_ticketing_time": "",
  "general_ticketing_date": "",
  "general_ticketing_time": ""
}}"""


class LyricsPrompts:
    """가사 번역/발음 관련 프롬프트"""
    
    @staticmethod
    def get_translation_prompt(lyrics: str, song_title: str = "", artist: str = "") -> str:
        """
        사용 위치: src/lyrics_translator.py -> translate_lyrics()
        목적: 영어 또는 일본어 가사를 한국어로 번역
        """
        return f"""아래의 "번역 규칙"과 "번역 예시"를 참고하여 주어진 "번역할 가사"를 한국어로 번역해주세요.

### 번역 규칙:
1. 가사에 포함된 모든 언어(영어, 일본어 등)를 한국어로 번역해야 합니다.
2. 가사 중간에 영어가 포함되어 있어도 반드시 한국어로 번역해야 합니다.
3. 예외: 'yeah', 'oh', 'wow'와 같이 의미가 거의 없는 짧은 추임새는 영어 그대로 유지할 수 있습니다.
4. 출력은 오직 번역된 한국어 가사만 포함해야 하며, 다른 어떤 설명도 추가하지 마세요.

### 번역 예시:
- 입력 가사: "You are my everything, 世界の中心で"
- 출력 결과: "너는 나의 전부, 세상의 중심에서"

### 번역할 가사:
{lyrics}
"""

    @staticmethod
    def get_pronunciation_prompt(lyrics: str, song_title: str = "", artist: str = "") -> str:
        """
        사용 위치: src/lyrics_translator.py -> convert_to_pronunciation()
        목적: 영어 가사 또는 일본어 가사를 한국어 발음으로 변환
        """
        return f"""다음 영어 가사 또는 일본어 가사를 한국어 발음으로 변환해주세요. 오직 한국어 발음만 출력하세요.

{lyrics}

중요: 다른 설명이나 추가 정보 없이 한국어 발음만 출력하세요."""

    @staticmethod
    def get_concert_introduction_prompt(title: str, artist_name: str) -> str:
        """콘서트 소개글 생성"""
        return f""""{artist_name}"의 "{title}" 공연에 대한 한국어 소개글을 2~3문장으로 작성해주세요.
공연명과 아티스트 정보를 바탕으로 자연스럽고 간결하게 작성하세요.

반드시 아래 JSON만 출력:
{{"introduction": "소개글 내용"}}"""

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

    @staticmethod
    def get_similar_artists_prompt(artist_name: str, songs: list) -> str:
        """
        사용 위치: src/gemini_api.py -> search_similar_artists()
        목적: 유사 아티스트 검색
        """
        song_list = ", ".join(songs)
        return f"""아래 아티스트와 음악 스타일이 유사한 다른 아티스트를 추천해주세요.

아티스트: {artist_name}
대표곡: {song_list}

추천 기준:
- 음악 장르, 분위기, 사용하는 악기 등을 고려해주세요.
- 최소 5팀의 아티스트를 추천해주세요.

JSON 형식으로만 응답해주세요:
{{"similar_artists": [{{"artist_name": "추천아티스트1"}}, {{"artist_name": "추천아티스트2"}}]}}
"""

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



