import discord
from discord.ext import commands

class AppilotBot:
    def __init__(self):
        intents = discord.Intents.default()  
        intents.messages = True  

        self.bot = commands.Bot(command_prefix='!', intents=intents)
        self.channel = None
        
        self.DISCORD_TOKEN = 'MTMyNjYyNjk3NTI4NDA3MjU1MA.GlLsaq.gVweNxx9HnxoRF1s3JXY2PsYe5440KLnayL3QE'
        self.channel = None
        self.guild = None
        
        @self.bot.event
        async def on_ready():
            print(f'{self.bot.user} has connected to Discord!')
            # Define server and channel IDs
            server_id = 1328759015261601894
            channel_id = 1354001745251926117
            
            # Send a welcome message when the bot connects
            message_data = {
                "server_id": server_id,
                "channel_id": channel_id,
                "message": "Bot has connected to Discord successfully!",
                "type": "info"
            }
            await self.send_message(message_data)
            
        @self.bot.event
        async def on_message(message):
            if message.author == self.bot.user:
                return
            await self.bot.process_commands(message)

    async def start_bot(self):
        """Start the Discord bot"""
        try:
            await self.bot.start(self.DISCORD_TOKEN)
        except Exception as e:
            print(f"Error starting Discord bot: {e}")

    async def send_message(self, message_data: dict):
        """
        Send a formatted message to Discord channel
        message_data should contain: message, task_id, job_id
        """
        
        server_id = message_data.get("server_id")
        channel_id = message_data.get("channel_id")
        type = message_data.get("type")
        
        self.guild = self.bot.get_guild(server_id)
        if not self.guild:
            print(f"Could not find server with ID: {server_id}")
            return
        
        self.channel = self.guild.get_channel(channel_id)
        if not self.channel:
            print(f"Could not find channel with ID: {channel_id}")
            return

        try:
            if type == "final":
                    embed = discord.Embed(
                    title="Task Final Update",
                    color=discord.Color.green()
                )
            elif type == "update":
                    embed = discord.Embed(
                    title="Task Update",
                    color=discord.Color.blue()
                )
            elif type == "error":
                    embed = discord.Embed(
                    title="Error",
                    color=discord.Color.red()
                )
            elif type == "info":
                    embed = discord.Embed(
                    title="Schedule",
                    color=discord.Color.dark_green()
                )
                    
            embed.add_field(name="Stats", value=message_data.get("message", "No message"), inline=False)
            
            await self.channel.send(embed=embed)
            
        except Exception as e:
            print(f"Error sending message to Discord: {e}")

# Create a singleton instance
bot_instance = AppilotBot()


# # discord_bot.py
# import discord
# from discord.ext import commands
# import redis_client
# import json
# import asyncio
# from redis_client import get_redis_client
# import os



# intents = discord.Intents.all()  # Or more specifically
# intents = discord.Intents.default()
# intents.message_content = True
# intents.messages = True
# intents.members = True
# bot = commands.Bot(command_prefix='!', intents=intents)

# redis_client = get_redis_client()

# DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
# if not DISCORD_BOT_TOKEN:
#     raise ValueError("No Discord bot token provided")

# @bot.event
# async def on_ready():
#     print(f'{bot.user} has connected to Discord!')

# async def listen_for_messages():
#     pubsub = redis_client.pubsub()
#     await pubsub.subscribe("discord_bot_channel")
#     print("Listening for messages from Redis...")
    
#     try:
#         async for message in pubsub.listen():
#             if message['type'] == 'message':
#                 try:
#                     message_data = json.loads(message['data'])
#                     print(f"Received message: {message_data}")
#                     await send_discord_message(message_data)
#                 except Exception as e:
#                     print(f"Error processing message: {e}")
#     except Exception as e:
#         print(f"Error in pubsub listener: {e}")
#     finally:
#         await pubsub.unsubscribe("discord_bot_channel")
#         await redis_client.close()



# async def send_discord_message(message_data):
#     server_id = int(message_data.get("server_id") or message_data.get("serverId"))
#     channel_id = int(message_data.get("channel_id") or message_data.get("channelId"))
#     content = message_data.get("message", "No message")
#     message_type = message_data.get("type")

#     print(f"Attempting to send message to server {server_id}, channel {channel_id}")
#     try:
#         guild = bot.get_guild(server_id)
#         if not guild:
#             print(f"Could not find server with ID: {server_id}")
#             return
        
#         channel = guild.get_channel(channel_id)
#         if not channel:
#             print(f"Could not find channel with ID: {channel_id}")
#             return
    
#         embed = discord.Embed(
#                 title=next((
#                     "Task Final Update" if message_type == "final" else
#                     "Task Update" if message_type == "update" else
#                     "Error" if message_type == "error" else
#                     "Schedule" if message_type == "info" else
#                     "Message"
#                 ), color=next((
#                     discord.Color.green() if message_type == "final" else
#                     discord.Color.blue() if message_type == "update" else
#                     discord.Color.red() if message_type == "error" else
#                     discord.Color.dark_green() if message_type == "info" else
#                     discord.Color.light_grey()
#                 ))
#             ))
        
#         embed.add_field(name="Details", value=content, inline=False)
#         await channel.send(embed=embed)
        
#         print(f"Message sent successfully to {channel.name}")
#     except discord.errors.Forbidden as e:
#         print(f"Permission error sending message: {e}")
#     except discord.errors.HTTPException as e:
#         print(f"HTTP error sending message: {e}")
#     except Exception as e:
#         print(f"Unexpected error sending message: {e}")

# async def main():
#     print("Starting Discord bot main function...")
#     try:
#         await asyncio.gather(
#             bot.start(DISCORD_BOT_TOKEN), 
#             listen_for_messages()
#         )
#     except Exception as e:
#         print(f"Error in main Discord bot function: {e}")

# if __name__ == "__main__":
#     asyncio.run(main())



# # discord_bot.py
# import discord
# from discord.ext import commands
# from redis_client import get_redis_client
# import json
# import asyncio
# import os
# from logger import logger

# redis_client = get_redis_client()
# # Initialize the Discord bot client
# intents = discord.Intents.default()
# intents.message_content = True
# bot = commands.Bot(command_prefix="!", intents=intents)

# # Initialize Redis client (shared among all workers)

# channel_name = "discord_bot_channel"

# DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
# if not DISCORD_BOT_TOKEN:
#     raise ValueError("No Discord bot token provided")


# @bot.event
# async def on_ready():
#     print(f'{bot.user} has connected to Discord!')
#     logger.info(f'Discord bot started and connected as {bot.user}')

# # Function to handle messages from Redis
# async def listen_for_messages():
#     pubsub = redis_client.pubsub()
#     pubsub.subscribe(channel_name)
#     print("Listening for messages from Redis...")

#     try:
#         while True:
#             message = pubsub.get_message()
#             if message and message['type'] == 'message':
#                 message_data = json.loads(message['data'])
#                 print(f"Received message: {message_data}")
#                 await send_discord_message(message_data)
#             await asyncio.sleep(0.1)  # Prevent blocking
#     except Exception as e:
#         print(f"Error in pubsub listener: {e}")
#     finally:
#         await pubsub.unsubscribe(channel_name)

# # Function to send messages to Discord
# async def send_discord_message(message_data):
#     server_id = int(message_data['server_id'])
#     channel_id = int(message_data['channel_id'])
#     content = message_data['message']

#     guild = bot.get_guild(server_id)
#     if not guild:
#         print(f"Guild not found: {server_id}")
#         return

#     channel = guild.get_channel(channel_id)
#     if not channel:
#         print(f"Channel not found: {channel_id}")
#         return

#     await channel.send(content)
#     print(f"Message sent to {channel.name}")

# # Run the bot client and listen for messages
# async def main():
#     await asyncio.gather(
#         bot.start(DISCORD_BOT_TOKEN), 
#         listen_for_messages()
#     )

# if __name__ == "__main__":
#     asyncio.run(main())
