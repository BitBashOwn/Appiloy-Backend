import discord
from discord.ext import commands
import os
import asyncio

DISCORD_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")


class AppilotBot:
    def __init__(self):
        intents = discord.Intents.default()
        intents.messages = True

        self.bot = commands.Bot(command_prefix="!", intents=intents)
        self.channel = None

        self.DISCORD_TOKEN = DISCORD_TOKEN
        self.channel = None
        self.guild = None
        
        # Create message queue for handling messages in the main bot loop
        self.message_queue = asyncio.Queue()
        self.queue_processor_task = None

        @self.bot.event
        async def on_ready():
            print(f"{self.bot.user} has connected to Discord!")
            # Start the message queue processor
            if self.queue_processor_task is None:
                self.queue_processor_task = self.bot.loop.create_task(self.process_message_queue())
                print("Message queue processor started")
            
            # Define server and channel IDs
            server_id = 1328759015261601894
            channel_id = 1354001745251926117

            # Send a welcome message when the bot connects
            message_data = {
                "server_id": server_id,
                "channel_id": channel_id,
                "message": "Bot has connected to Discord successfully!",
                "type": "info",
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

    async def process_message_queue(self):
        """Process messages from the queue in the main bot event loop"""
        print("Message queue processor running")
        while True:
            try:
                message_data = await self.message_queue.get()
                print(f"Processing message from queue: {message_data.get('type')}")
                
                server_id = message_data.get("server_id")
                channel_id = message_data.get("channel_id")
                message_type = message_data.get("type")

                guild = self.bot.get_guild(server_id)
                if not guild:
                    print(f"Could not find server with ID: {server_id}")
                    self.message_queue.task_done()
                    continue

                channel = guild.get_channel(channel_id)
                if not channel:
                    print(f"Could not find channel with ID: {channel_id}")
                    self.message_queue.task_done()
                    continue

                try:
                    if message_type == "final":
                        embed = discord.Embed(
                            title="Task Final Update", color=discord.Color.green()
                        )
                    elif message_type == "update":
                        embed = discord.Embed(title="Task Update", color=discord.Color.blue())
                    elif message_type == "error":
                        embed = discord.Embed(title="Error", color=discord.Color.red())
                    elif message_type == "info":
                        embed = discord.Embed(
                            title="Schedule", color=discord.Color.dark_green()
                        )

                    embed.add_field(
                        name="Stats",
                        value=message_data.get("message", "No message"),
                        inline=False,
                    )

                    await channel.send(embed=embed)
                    print(f"Message sent to Discord channel {channel_id}")

                except Exception as e:
                    print(f"Error sending message to Discord: {e}")
                
                self.message_queue.task_done()
            except Exception as e:
                print(f"Error in message queue processor: {e}")

    async def send_message(self, message_data: dict):
        """
        Queue a message to be sent to Discord channel in the main bot loop
        """
        # Just add to queue, actual sending happens in process_message_queue
        await self.message_queue.put(message_data)
        print(f"Message queued: {message_data.get('type')}")

    def send_message_sync(self, message_data: dict):
        """
        Synchronous interface to queue a message - safe to call from any thread
        """
        # Use run_coroutine_threadsafe to safely call from any thread
        if hasattr(self, 'bot') and self.bot.loop:
            future = asyncio.run_coroutine_threadsafe(
                self.send_message(message_data), 
                self.bot.loop
            )
            # Optionally wait for queue confirmation (not message sending)
            try:
                future.result(timeout=5)  # Short timeout just for queueing
                return True
            except Exception as e:
                print(f"Error queueing message: {e}")
                return False
        else:
            print("Bot or bot loop not available")
            return False


bot_instance = AppilotBot()

def get_bot_instance():
    return bot_instance