import asyncio
from Bot.discord_bot import bot_instance

async def run_bot():
    try:
        print("Starting Discord bot...")
        await bot_instance.start_bot()
    except Exception as e:
        print(f"Error starting Discord bot: {e}")

if __name__ == "__main__":
    asyncio.run(run_bot())
