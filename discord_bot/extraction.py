"""
콘서트 요청 정보에서 공연명/아티스트명/날짜를 추출하는 로직
"""
import discord
from datetime import date
from lib.prompts import DataCollectionPrompts



def build_extraction_prompt(concert_title: str, additional_info: str = "", crawled_text: str = "") -> str:
    today = date.today().isoformat()

    sources_block = f"""
[정보 소스 - 우선순위 순서대로 나열됨]
1순위 (크롤링 원문): {crawled_text if crawled_text else "(없음)"}
2순위 (추가 요청): {additional_info if additional_info else "(없음)"}
3순위 (콘서트명): {concert_title}
"""

    return f"""{DataCollectionPrompts.COMMON_SOURCE_RULES}

아래 정보를 바탕으로 이 공연의 공연명, 아티스트명, 공연날짜를 추출하세요.

{sources_block}

🔍 **추출 규칙:**
1. 정보 소스는 우선순위 순서대로 신뢰하세요 (크롤링 원문 > 추가 요청 > 콘서트명 단독 추론)
2. 콘서트명만으로 정보가 부족하면 웹 검색으로 보완하세요 (실제 존재하는 공연인지 반드시 확인)
3. 오늘 날짜는 {today}입니다. 이 날짜 이전 공연은 지난 공연이므로 날짜를 null로 처리하세요
4. 실존 여부를 확인할 수 없으면 (검색해도 안 나오면) 절대 추측하지 말고 null로 반환하세요
5. 아티스트명은 반드시 "영문 (한국어)" 형식으로 반환하세요 (예: "HONNE (혼네)"). 국내 아티스트는 한글만 반환하세요. 페스티벌 등 여러 아티스트가 출연하는 공연이면 artist_name은 null로, reason은 "festival_multi_artist"로 반환하세요
6. 정보 소스끼리 내용이 상충하면 우선순위 높은 쪽을 따르되, 상충했다는 사실을 note에 기록하세요
7. 날짜에 범위가 있으면(예: 이틀간 공연) start_date, end_date 둘 다 채우세요. 단일 날짜면 둘 다 같은 값으로 채우세요
8. 아티스트가 대한민국 국적이거나(국내 아티스트), 장르가 재즈/EDM(일렉트로닉 댄스뮤직)/클래식/오케스트라 계열이면 concert_title과 artist_name을 null로 하고 reason을 "unsupported_genre"로 반환하세요

각 필드마다 반드시 출처(source)와 신뢰도(confidence)를 함께 반환하세요.
- source는 다음 중 하나: "crawled" / "additional_info" / "search" / "title_only"
- confidence는 다음 중 하나: "high" / "medium" / "low"

null인 경우 reason에 아래 중 하나로 이유를 명시하세요:
- "not_found" (검색해도 실존 확인 불가)
- "ambiguous" (너무 일반적인 이름이라 특정 공연 특정 불가)
- "past" (실존하지만 날짜가 이미 지남)
- "festival_multi_artist" (페스티벌 등 여러 아티스트가 출연해 단일 아티스트로 특정 불가)
- "unsupported_genre" (국내 아티스트/공연이거나, 재즈·EDM·클래식·오케스트라 등 지원하지 않는 장르)
- "" (null이 아니면 빈 문자열)

반드시 아래 JSON 형식으로만 응답하세요:
{{
  "concert_title": {{"value": "정식 공연명 또는 null", "source": "...", "confidence": "..."}},
  "artist_name": {{"value": "영문 (한국어) 형식 또는 null", "source": "...", "confidence": "..."}},
  "start_date": {{"value": "YYYY-MM-DD 또는 null", "source": "...", "confidence": "..."}},
  "end_date": {{"value": "YYYY-MM-DD 또는 null", "source": "...", "confidence": "..."}},
  "reason": "null 필드가 있을 경우 사유 (not_found/ambiguous/past/festival_multi_artist), 없으면 빈 문자열",
  "conflict_note": "정보 소스 간 상충이 있었다면 설명, 없으면 빈 문자열"
}}"""


def format_result_embed(parsed: dict) -> discord.Embed:
    """LLM 추출 결과를 디스코드 임베드로 변환"""
    embed = discord.Embed(title="🔍 추출 결과", color=0x5865F2)

    def field_line(field: dict) -> str:
        value = field.get("value")
        if value is None:
            return "❌ 없음"
        source_label = {
            "crawled": "크롤링 원문",
            "additional_info": "추가 요청",
            "search": "검색결과",
            "title_only": "제목 단독추론",
            "manual": "수동입력",
        }.get(field.get("source", ""), field.get("source", ""))
        return f"{value}\n(출처: {source_label}, 신뢰도: {field.get('confidence', '')})"

    embed.add_field(name="공연명", value=field_line(parsed["concert_title"]), inline=False)
    embed.add_field(name="아티스트명", value=field_line(parsed["artist_name"]), inline=False)

    start = parsed["start_date"].get("value")
    end = parsed["end_date"].get("value")
    if start and end:
        date_str = start if start == end else f"{start} ~ {end}"
        embed.add_field(name="공연날짜", value=f"{date_str}\n(신뢰도: {parsed['start_date'].get('confidence', '')})", inline=False)
    else:
        embed.add_field(name="공연날짜", value="❌ 없음", inline=False)

    reason_label = {
        "not_found": "⚠️ 실존 확인 불가",
        "ambiguous": "⚠️ 특정 공연 특정 불가 (명칭 모호)",
        "past": "⚠️ 과거 공연으로 제외됨",
        "festival_multi_artist": "⚠️ 페스티벌 - 단일 아티스트 특정 불가",
        "unsupported_genre": "⚠️ 지원하지 않는 장르 (국내 아티스트 또는 재즈/EDM/클래식)",
    }
    reason = parsed.get("reason", "")
    if reason:
        embed.add_field(name="예상 결과", value=reason_label.get(reason, reason), inline=False)

    conflict = parsed.get("conflict_note", "")
    if conflict:
        embed.add_field(name="⚠️ 정보 상충", value=conflict, inline=False)
    
    reason = parsed.get("reason", "")
    embed.set_footer(text=f"reason_code:{reason}")
    return embed
