import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import json
import asyncio
import discord
from discord import app_commands
from discord.ext import commands
from lib.config import Config
from lib.data_collector import DataCollector
from core.apis.gemini_api import GeminiAPI
from core.apis.instagram_api import InstagramAPI
from utils import get_request_info, find_latest_extraction_message
from extraction import build_extraction_prompt, format_result_embed
from registration import register_concert

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)
gemini_api = GeminiAPI(Config.GEMINI_API_KEY)
data_collector = DataCollector(gemini_api) 
ig_api = InstagramAPI(Config.INSTAGRAM_USERNAME, Config.INSTAGRAM_PASSWORD)


@bot.event
async def on_ready():
    print(f"✅ 로그인 완료: {bot.user}")
    synced = await bot.tree.sync()
    print(f"슬래시 커맨드 {len(synced)}개 동기화 완료")


@bot.tree.command(name="테스트", description="봇 연결 확인용")
async def test_command(interaction: discord.Interaction):
    await interaction.response.send_message("봇이 정상적으로 연결됐어요! 🎉")


@bot.tree.command(name="추출", description="요청 정보에서 공연명/아티스트명/날짜를 AI로 추출합니다")
async def extract_command(interaction: discord.Interaction):
    if not isinstance(interaction.channel, discord.Thread):
        await interaction.response.send_message(
            "이 명령어는 포럼 포스트(스레드) 안에서만 사용할 수 있어요.", ephemeral=True
        )
        return

    await interaction.response.defer()

    try:
        info = await get_request_info(interaction.channel)
    except Exception as e:
        await interaction.followup.send(f"원본 요청 정보를 읽는 데 실패했어요: {e}")
        return

    concert_title = info.get("concert_title", "")
    additional_info = info.get("additional_info", "")
    url = info.get("url", "")

    crawled_text = ""
    if url and "instagram.com" in url:
        post = ig_api.fetch_post_by_url(url)
        if post:
            crawled_text = post.caption

    prompt = build_extraction_prompt(concert_title, additional_info, crawled_text)

    try:
        result = gemini_api.query_json(prompt, use_search=True)
    except Exception as e:
        await interaction.followup.send(f"AI 추출 중 오류가 발생했어요: {e}")
        return

    if not result:
        await interaction.followup.send("AI 추출 결과를 받지 못했어요. 다시 시도해주세요.")
        return

    embed = format_result_embed(result)
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="수정", description="추출된 정보 중 일부를 수동으로 수정합니다")
@app_commands.describe(
    공연명="수정할 공연명 (선택)",
    아티스트명="수정할 아티스트명 (선택)",
    날짜="수정할 날짜, YYYY-MM-DD 형식. 범위면 '시작~종료' (선택)"
)
async def edit_command(
    interaction: discord.Interaction,
    공연명: str = None,
    아티스트명: str = None,
    날짜: str = None
):
    if not isinstance(interaction.channel, discord.Thread):
        await interaction.response.send_message("스레드 안에서만 사용할 수 있어요.", ephemeral=True)
        return

    if not any([공연명, 아티스트명, 날짜]):
        await interaction.response.send_message("수정할 필드를 하나 이상 입력해주세요.", ephemeral=True)
        return

    msg = await find_latest_extraction_message(interaction.channel, bot.user)
    if msg is None:
        await interaction.response.send_message("먼저 `/추출`을 실행해주세요.", ephemeral=True)
        return

    embed = msg.embeds[0]

    if 공연명:
        embed.set_field_at(0, name="공연명", value=f"{공연명}\n(출처: 수동입력)", inline=False)
    if 아티스트명:
        embed.set_field_at(1, name="아티스트명", value=f"{아티스트명}\n(출처: 수동입력)", inline=False)
    if 날짜:
        embed.set_field_at(2, name="공연날짜", value=f"{날짜}\n(출처: 수동입력)", inline=False)

    await msg.edit(embed=embed)
    await interaction.response.send_message("✏️ 수정 완료했어요.")

@bot.tree.command(name="추가", description="확정된 정보로 콘서트를 등록합니다")
async def add_command(interaction: discord.Interaction):
    if not isinstance(interaction.channel, discord.Thread):
        await interaction.response.send_message("스레드 안에서만 사용할 수 있어요.", ephemeral=True)
        return

    await interaction.response.defer()

    msg = await find_latest_extraction_message(interaction.channel, bot.user)
    if msg is None:
        await interaction.followup.send("먼저 `/추출`을 실행해주세요.")
        return

    info = await get_request_info(interaction.channel)
    request_id = int(info["request_id"])

    # DB 연결 (SSH 터널)
    from tools.database.ssh_mysql_connection import SSHMySQLConnection
    ssh_config = {
        'host': Config.DB_SSH_HOST, 'port': Config.DB_SSH_PORT,
        'username': Config.DB_SSH_USER, 'private_key_path': Config.get_ssh_key_path()
    }
    mysql_config = {
        'host': Config.DB_HOST, 'port': Config.DB_PORT,
        'user': Config.DB_USER, 'password': Config.DB_PASSWORD,
        'database': Config.DEV_DB_NAME, 'charset': 'utf8mb4'
    }
    db = SSHMySQLConnection(ssh_config, mysql_config)

    try:
        if not db.connect():
            await interaction.followup.send("DB 연결에 실패했어요.")
            return

        result = await asyncio.to_thread(
            register_concert, db, request_id, msg.embeds[0], data_collector, gemini_api
        )

        if result["success"]:
            d = result["detail"]
            lines = [f"✅ 등록 완료! concert_id: {result['concert_id']}"]
            lines.append(f"👤 아티스트: {d['artist_name']} ({'🆕 신규 생성' if d['artist_is_new'] else '기존 재사용'})")
            lines.append(f"🎫 콘서트: {d['concert_title']} ({'🆕 신규 생성' if d['concert_is_new'] else '기존 콘서트 재사용'})")
            if d['concert_schedule_added']:
                lines.append(f"📅 공연 일정 추가: {', '.join(d['concert_schedule_added'])}")
            if d['genres_added']:
                lines.append(f"🎵 장르 추가: {', '.join(d['genres_added'])}")
            if d['ticketing_added']:
                lines.append(f"🎟️ 예매 일정 추가: {', '.join(d['ticketing_added'])}")
            else:
                lines.append("🎟️ 예매 일정: 확인된 정보 없음")
            await interaction.followup.send("\n".join(lines))

            # 자동등록여부 TRUE면 관심콘서트 등록
            if info.get("auto_register"):
                db.cursor.execute(
                    "SELECT id FROM user_interest_concerts WHERE user_id = %s AND concert_id = %s",
                    (int(info["user_id"]), result["concert_id"])
                )
                if db.cursor.fetchone():
                    await interaction.followup.send("ℹ️ 이미 관심 콘서트로 등록되어 있어요.")
                else:
                    db.cursor.execute("""
                        INSERT INTO user_interest_concerts
                            (user_id, concert_id, concert_title, user_nickname, toast_shown, alarm_check, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, 0, 0, NOW(3), NOW(3))
                    """, (
                        int(info["user_id"]), result["concert_id"],
                        info.get("concert_title", ""), info.get("user_nickname", "")
                    ))
                    db.commit()
                    await interaction.followup.send("➕ 관심 콘서트로도 등록했어요.")
        else:
            await interaction.followup.send(f"❌ 등록 실패: {result['request_result']}")
    finally:
        db.disconnect()


bot.run(Config.DISCORD_BOT_TOKEN)