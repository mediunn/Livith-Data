import discord


async def get_request_info(thread: discord.Thread) -> dict:
    """스레드의 첫 메시지(요청 알림 임베드)에서 정보 추출"""
    starter_message = thread.starter_message
    if starter_message is None:
        starter_message = await thread.fetch_message(thread.id)

    if not starter_message.embeds:
        raise ValueError("요청 알림 임베드를 찾을 수 없습니다")

    embed = starter_message.embeds[0]
    info = {}

    for field in embed.fields:
        name = field.name.strip()
        value = field.value.strip()

        if "요청 ID" in name:
            info["request_id"] = value
        elif "유저 ID" in name:
            info["user_id"] = value
        elif "유저 닉네임" in name:
            info["user_nickname"] = value
        elif "자동 등록 여부" in name:
            info["auto_register"] = value.lower() == "true"
        elif "콘서트명" in name:
            info["concert_title"] = value
        elif "URL" in name:
            info["url"] = value
        elif "추가 요청" in name:
            info["additional_info"] = value

    return info  # ← 이게 get_request_info 안으로 다시 들어와야 함


async def debug_raw_fields(thread: discord.Thread) -> str:
    """임베드 필드명/값을 원본 그대로 출력 (디버깅용)"""
    starter_message = thread.starter_message
    if starter_message is None:
        starter_message = await thread.fetch_message(thread.id)
    embed = starter_message.embeds[0]
    return "\n".join(f"[{repr(f.name)}] = [{repr(f.value)}]" for f in embed.fields)


async def find_latest_extraction_message(thread: discord.Thread, bot_user) -> discord.Message:
    """스레드에서 봇이 올린 가장 최근 '추출 결과' 임베드 메시지 찾기"""
    async for message in thread.history(limit=50):
        if message.author.id == bot_user.id and message.embeds:
            if message.embeds[0].title == "🔍 추출 결과":
                return message
    return None