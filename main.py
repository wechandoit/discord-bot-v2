# the os module helps us access environment variables
# i.e., our API keys
import os
from dotenv import load_dotenv

# the Discord Python API
import nextcord as discord
from nextcord.ext import commands

# multithreading
import asyncio
import nest_asyncio

nest_asyncio.apply()
load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')

intents = discord.Intents.default()
intents.message_content = True
client = commands.Bot(command_prefix='$', intents=intents)

GUILD_IDS = (
    [int(guild_id) for guild_id in client.guilds]
    if client.guilds
    else discord.utils.MISSING
)  
    
@client.event
async def on_ready():
    print('------')
    print(f'Logged in as {client.user.name}')
    print(client.user.id)
    print(f'In {len(client.guilds)} servers')
    print(f'{[guild.name for guild in client.guilds]}')
    print('------')
    
    await client.change_presence(activity=None)
    
async def change_status():
    await client.wait_until_ready()
    while not client.is_closed():
        await client.change_presence(activity=discord.Activity(type=discord.ActivityType.listening, name=f' {len(client.guilds)} servers'), status=discord.Status.dnd)   
        await asyncio.sleep(10)

async def load():
    for filename in os.listdir('./cogs'):
        if filename.endswith('.py'):
            client.load_extension(f'cogs.{filename[:-3]}')

async def main():
    await load()
    client.loop.create_task(change_status())
    client.run(TOKEN)


if __name__ == '__main__':
    asyncio.run(main())
