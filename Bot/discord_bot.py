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
            self.channel = self.bot.get_channel(self.CHANNEL_ID)
            if not self.channel:
                print(f"Could not find channel with ID: {self.CHANNEL_ID}")
            
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