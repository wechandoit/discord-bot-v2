from pymongo.mongo_client import MongoClient

import os
from dotenv import load_dotenv

from aiohttp import request
import asyncio
import time
from datetime import datetime

import pandas as pd
import numpy as np
import pyspark.pandas as ps
from pyspark.sql import SparkSession

import json

load_dotenv()

API_KEY = os.getenv('VAL_API_KEY_2')
uri = os.getenv('MONGODB_URI')
headers = {
    "Accept": "application/json",
    "Authorization": f"{API_KEY}"
}

def get_mmr_change_emoji(change: int) -> str:
    if change > 0:
        return '<:increase:1302749785652203581>'
    elif change < 0:
        return '<:decrease:1302749787950682195>'
    else:
        return '<:neutral:1302749784125472859>'

def get_team_emoji(team_name: str) -> str:
    if team_name == 'Blue':
        return '🟦'
    if team_name == 'Red':
        return '🟥'
    
def update_match_in_db(data, match_data, match_uuid, matchesdb):
    data = data['data']
    metadata = data['metadata']
    players = data['players']
    teams = data['teams']

    match_players = []
    for player_index in range(len(players)):
        player_data = players[player_index]
        player_stats = player_data['stats']
        ability_casts = player_data['ability_casts']
        match_player_obj = match_player(player_data['name'], player_data['tag'], player_data['team_id'], player_data['agent']['name'].lower(), player_stats['kills'], player_stats['deaths'], player_stats['score'], player_stats['assists'], player_stats['headshots'], player_stats['bodyshots'], player_stats['legshots'], ability_casts['grenade'], ability_casts['ability1'], ability_casts['ability2'], ability_casts['ultimate'], player_data['tier']['name'])
        match_players.append(match_player_obj)
    match_obj = comp_match(match_data['map_name'], match_data['server'], match_data['match_id'], match_data['blue_score'], match_data['red_score'], match_data['who_won'], match_players, None, None, match_data['start_date'], match_data['end_date'])
    
    who_won = None
    for team_index in range(len(teams)):
        team_data = teams[team_index]
        if team_data['team_id'] == 'Red':
            if team_data['won'] == True:
                who_won = 'Red'
        elif team_data['team_id'] == 'Blue':
            if team_data['won'] == True:
                who_won = 'Blue'
    if who_won == None:
        who_won = 'Tie'

    player_data_list = []
    for p in match_players:
        player_data = {
            "name": p.player_name,
            "tag": p.player_tag,
            "team_id": p.team_id,
            "agent": p.agent_name,
            "kills": p.kills,
            "deaths": p.deaths,
            "score": p.score,
            "assists": p.assists,
            "headshots": p.headshots,
            "bodyshots": p.bodyshots,
            "legshots": p.legshots,
            "ability_casts": {
                "grenade": p.e_ability,
                "ability1": p.c_ability,
                "ability2": p.q_ability,
                "ultimate": p.x_ability
            },
            "tier": p.rank_in_match
        }
        player_data_list.append(player_data)
        
    match_data = {
        "map_name": match_obj.map_name,
        "server": match_obj.server,
        "match_id": match_obj.match_id,
        "blue_score": match_obj.blue_score,
        "red_score": match_obj.red_score,
        "who_won": who_won,
        "match_players": player_data_list,
        "start_date": match_obj.start_date,
        "end_date": match_obj.end_date
    }

    matchesdb.Match_Data.update_one({'match_id': match_uuid}, {'$set' : match_data})

async def main():
    mongo = MongoClient(uri)
    spark = SparkSession.builder.getOrCreate()

    playersdb = None
    matchesdb = None
    
    try:
        mongo.admin.command('ping')
        print("[Updater] Pinged the MongoDB database successfully!")

        if 'players' in mongo.list_database_names():
            playerdb = mongo['players']
        else:
            playerdb = mongo['players']
            temp_player_df = pd.DataFrame({'player': [''], 'puuid': [''], 'last_updated': [''], 'region': ['']})
            playerdb.Players.insert_many(temp_player_df.to_dict("records"))
            
        if 'matches' in mongo.list_database_names():
            matchesdb = mongo['matches']
        else:
            matchesdb = mongo['matches']
            temp_matches_df = pd.DataFrame({'match_id': [''], 'match_json': ['']})
            matchesdb.Match_Data.insert_many(temp_matches_df.to_dict("records"))
    except Exception as e:
            print("[Updater] Could not connect to the MongoDB database...")
            print(e)
            return
    
    # Get matches dataframe as pandas then convert to pyspark dataframe
    matches_pdf = pd.DataFrame(list(matchesdb.Match_Data.find()), columns=['map_name', 'server', 'match_id', 'blue_score', 'red_score', 'who_won', 'match_players', 'start_date', 'end_date'])
    matches_sdf = spark.createDataFrame(matches_pdf)

    match_ids = matches_sdf.select('match_id').rdd.flatMap(lambda row: row).collect()

    total_matches = len(match_ids)

    # See if we need to update the collection because of missing rows/values
    '''count = 1
    for match_uuid in match_ids:
        match_row = matches_sdf[matches_sdf['match_id'] == match_uuid]
        match_data = match_row.collect()[0]

        region = None
        EU_List = ['Stockholm', 'Frankfurt']
        if (str(match_data['server'][:2]) == 'US'): region = 'na'
        elif (str(match_data['server']) in EU_List): region = 'eu'

        if (str(match_data['who_won']).lower() == 'nan'):
            print(f'({count}/{total_matches}) Updating {match_uuid}... row: who_won does not exist')
            async with request('GET', f'https://api.henrikdev.xyz/valorant/v4/match/{region}/{match_uuid}', headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    update_match_in_db(data, match_data, match_uuid, matchesdb)
                elif response.status == 429:
                    print(f'({count}/{total_matches}) [Updater] rate limit reached, waiting 60 seconds for it to refresh!')
                    time.sleep(60)
                    async with request('GET', f'https://api.henrikdev.xyz/valorant/v4/match/{region}/{match_uuid}', headers=headers) as response:
                        if response.status == 200:
                            update_match_in_db(data, match_data, match_uuid, matchesdb)
                        else:
                            print(f'({count}/{total_matches})Status {response.status} for match id: {match_uuid}')
                else:
                    print(f'({count}/{total_matches})Status {response.status} for match id: {match_uuid}')
        else:
            print(f'({count}/{total_matches}) Skipping {match_uuid}, object up to date!')
        count += 1'''

    # Do stats: see how many kills the match mvp had on the winning team on average vs. rounds played
    count = 1
    for match_uuid in match_ids:
        match_row = matches_sdf[matches_sdf['match_id'] == match_uuid]
        match_data = match_row.collect()[0]

        # Get total rounds played
        total_rounds_played = match_data['red_score'] + match_data['blue_score']

        match_players = []
        for player_data in match_data["match_players"]:
            ability_casts = {}
            for element in player_data["ability_casts"][1:len(player_data["ability_casts"])-1].split(','):
                key, value = element.strip().split('=', 1)
                try:
                    ability_casts[key] = int(value)
                except Exception:
                    ability_casts[key] = 0

            match_player_obj = match_player(player_data["name"], player_data["tag"], player_data["team_id"], player_data["agent"], player_data["kills"], player_data["deaths"], player_data["score"], player_data["assists"], player_data["headshots"], player_data["bodyshots"], player_data["legshots"], ability_casts["grenade"], ability_casts["ability1"], ability_casts["ability2"], ability_casts["ultimate"], player_data["tier"])
            match_players.append(match_player_obj)

            print(f'({count}/{total_matches}) {match_uuid} | {total_rounds_played} | {match_player_obj.kills} | {round(match_player_obj.kills/total_rounds_played, 2)} | {round(match_player_obj.get_kda(), 2)} | {match_player_obj.agent_name}')
        count += 1
    
    # Generate histogram of total rounds played vs. match mvp kills/total rounds played ratio


class match_player():
    def __init__(self, player_name, player_tag, team_id, agent_name, kills, deaths, score, assists, headshots, bodyshots, legshots, e_ability, c_ability, q_ability, x_ability, rank_in_match):
        self.player_name = player_name
        self.player_tag = player_tag
        self.team_id = team_id
        self.agent_name = agent_name
        self.kills = int(kills)
        self.deaths = int(deaths)
        self.score = int(score)
        self.assists = int(assists)
        self.headshots = int(headshots)
        self.bodyshots = int(bodyshots)
        self.legshots = int(legshots)
        self.e_ability = int(e_ability)
        self.c_ability = int(c_ability)
        self.q_ability = int(q_ability)
        self.x_ability = int(x_ability)
        self.rank_in_match = rank_in_match

    def get_full_tag(self):
        return self.player_name + "#" + self.player_tag

    def get_headshot_percentage(self):
        return float(self.headshots) / (self.headshots + self.bodyshots + self.legshots)

    def get_kd(self):
        if self.deaths != 0:
            return float(self.kills) / self.deaths
        else:
            return self.kills

    def get_kda(self):
        if self.deaths != 0:
            return float(self.kills + self.assists) / self.deaths
        else:
            return self.kills + self.assists

    def get_kda_string(self):
        return f'{self.kills}-{self.deaths}-{self.assists}'

    def get_agent_emoji(self):
        agent_emojis = {
            "astra": "<:astra:1302748669170159636>",
            "breach": "<:breach:1302748702045376584>",
            "brimstone": "<:brimstone:1302748741333286944>",
            "chamber": "<:chamber:1302748772454895667>",
            "clove": "<:clove:1302748809352314924>",
            "cypher": "<:cypher:1302748834111422505>",
            "deadlock": "<:deadlock:1302748861172813914>",
            "fade": "<:fade:1302748892001206283>",
            "gekko": "<:gekko:1302748919708782703>",
            "harbor": "<:harbor:1302748955263897640>",
            "iso": "<:iso:1302748985488048191>",
            "jett": "<:jett:1302749011337543731>",
            "kay/o": "<:kayo:1302749040861253642>",
            "killjoy": "<:killjoy:1302749085161492531>",
            "neon": "<:neon:1302749111942123570>",
            "omen": "<:omen:1302749144661758032>",
            "phoenix": "<:phoenix:1302749172042301480>",
            "raze": "<:raze:1302749193160626176>",
            "reyna": "<:reyna:1302749217248383036>",
            "sage": "<:sage:1302749239331389460>",
            "skye": "<:skye:1302749261720588349>",
            "sova": "<:sova:1302749294113067038>",
            "viper": "<:viper:1302749317748232273>",
            "vyse": "<:vyse:1302750003609075733>",
            "yoru": "<:yoru:1302749345845870643>"
        }

        return agent_emojis.get(self.agent_name, "")  # Return empty string if agent not found

class comp_match():
    def __init__(self, map_name, server, match_id, blue_score, red_score, who_won, match_players, lookup_team, lookup_player, start_date: datetime, end_date: datetime):
        self.map_name = map_name
        self.server = server
        self.match_id = match_id
        self.blue_score = int(blue_score)
        self.red_score = int(red_score)
        self.match_players = match_players
        self.lookup_team = lookup_team
        self.lookup_player = lookup_player
        self.start_date = start_date
        self.end_date = end_date
        if who_won == 'Blue':
            self.winner = 'Blue'
        elif who_won == 'Red':
            self.winner = 'Red'
        else:
            self.winner = 'Tie'

    def get_score(self):
        if self.lookup_team == 'Blue':
            return f'{self.blue_score}-{self.red_score}'
        else:
            return f'{self.red_score}-{self.blue_score}'

    def get_formatted_map(self):
        formatted_map = []
        start_date_timestamp = int(self.start_date.timestamp())
        end_date_timestamp = int(self.end_date.timestamp())
        formatted_map.append(f'**{self.map_name} | {self.get_score()} | <t:{start_date_timestamp}:d><t:{start_date_timestamp}:t> - <t:{end_date_timestamp}:d><t:{end_date_timestamp}:t>**')

        # Sort players by team and KDA within each team
        blue_team = [player for player in self.match_players if player.team_id == 'Blue']
        red_team = [player for player in self.match_players if player.team_id == 'Red']

        blue_team.sort(key=lambda x: x.score / (self.blue_score + self.red_score), reverse=True)
        red_team.sort(key=lambda x: x.score / (self.blue_score + self.red_score), reverse=True)

        formatted_str = ""
        for team in [blue_team, red_team]:
            for player in team:
                if player.get_full_tag().lower() == self.lookup_player.lower():
                    formatted_str += f'**{get_team_emoji(player.team_id)}{player.get_agent_emoji()} | {player.get_full_tag()} | ACS: {player.score / (self.blue_score + self.red_score):.1f} | KDA: {player.get_kda_string()} | HS%: {player.get_headshot_percentage() * 100:.1f}%**\n'
                else:
                    formatted_str += f'{get_team_emoji(player.team_id)}{player.get_agent_emoji()} | {player.get_full_tag()} | ACS: {player.score / (self.blue_score + self.red_score):.1f} | KDA: {player.get_kda_string()} | HS%: {player.get_headshot_percentage() * 100:.1f}%\n'

        formatted_str += f'Server: {self.server}\n'
        formatted_map.append(formatted_str)
        return formatted_map
        
if __name__ == '__main__':
    asyncio.run(main())
