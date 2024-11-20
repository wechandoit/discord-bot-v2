import nextcord as discord
from nextcord.ext import commands
from nextcord import application_command as app_commands

import os
from dotenv import load_dotenv
load_dotenv()

from aiohttp import request

from datetime import datetime
from datetime import timedelta

import pandas as pd

from pymongo.mongo_client import MongoClient

API_KEY = os.getenv('VAL_API_KEY')
uri = os.getenv('MONGODB_URI')

headers = {
    "Accept": "application/json",
    "Authorization": f"{API_KEY}"
}

async def setup(client: commands.Bot):
    client.add_cog(val(client))

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

class val(commands.Cog):
    def __init__(self, client: commands.Bot):
        self.client = client

        self.mongo = MongoClient(uri)
        try:
            self.mongo.admin.command('ping')
            print("Pinged the MongoDB database successfully!")

            if 'players' in self.mongo.list_database_names():
                self.playerdb = self.mongo['players']
            else:
                self.playerdb = self.mongo['players']
                temp_player_df = pd.DataFrame({'player': [''], 'puuid': [''], 'last_updated': [''], 'region': ['']})
                self.playerdb.Players.insert_many(temp_player_df.to_dict("records"))
                
            if 'matches' in self.mongo.list_database_names():
                self.matchesdb = self.mongo['matches']
            else:
                self.matchesdb = self.mongo['matches']
                temp_matches_df = pd.DataFrame({'match_id': [''], 'match_json': ['']})
                self.matchesdb.Match_Data.insert_many(temp_matches_df.to_dict("records"))


        except Exception as e:
            print("Could not connect to the MongoDB database...")
            print(e)

    @app_commands.slash_command(name='val_stats', description='Get valorant stats for a player')
    async def val_stats(self, interaction: discord.Interaction, player):
        await interaction.response.defer()

        unfiltered_player_name = player.split('#')
        if len(unfiltered_player_name) != 2:
            await interaction.followup.send(f'Error: {player} is not a valid player name!')
            return

        player_name = unfiltered_player_name[0]
        player_tag = unfiltered_player_name[1]

        val_player_obj = None
        player_df = pd.DataFrame(list(self.playerdb.Players.find()), columns=['player', 'puuid', 'last_updated', 'region'])
        player_df.astype(str)

        last_updated = None
        should_update_match_history = False

        account_data_url = f'https://api.henrikdev.xyz/valorant/v2/account/{player_name}/{player_tag}?force=true'
        async with request('GET', account_data_url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                data = data['data']
                card_id, title_id = None, None
                val_player_obj = None
                
                if 'card' in data:
                    card_id = data['card']
                if 'title' in data:
                    title_id = data['title']

                val_player_obj = val_player(data['puuid'], player_name, player_tag, data['region'].upper(), int(data['account_level']), title_id, card_id)
                last_updated = data['updated_at']
            else:
                await interaction.followup.send(f'Error getting data for {player}: Status {response.status}')
                return

        if player_df['puuid'].str.contains(val_player_obj.puuid).any():
            player_df.loc[player_df.puuid == val_player_obj.puuid, 'player'] = str(player)
            delta = datetime.now() - datetime.strptime(player_df.loc[(player_df['puuid'] == val_player_obj.puuid), 'last_updated'].values[0], "%Y-%m-%dT%H:%M:%S.%fZ")
            if abs(delta.total_seconds()) > (5 * 60):
                should_update_match_history = True
            self.playerdb.Players.update_one({'puuid': val_player_obj.puuid}, {'$set' :{'player': player, 'last_updated': last_updated, 'region':val_player_obj.region}})
            print(f'{player} is in the player database! Attempting to update document...')
        else:
            individual_player_df = pd.DataFrame({'player':[str(player)], 'puuid':[val_player_obj.puuid], 'last_updated': [last_updated], 'region':[val_player_obj.region]})
            self.playerdb.Players.insert_many(individual_player_df.to_dict("records"))
            print(f'{player} saved to database!')

        # if puuid of the val player is in the database
        mmr_changes, match_maps, match_ids, match_times = [], [], [], []
        account_rank_history, account_rr_history, account_rank_url_history = [], [], []
        match_info = []
        max_count = 0
        if f'{val_player_obj.puuid}' in self.mongo.list_database_names():
            print(f'Getting match history for {player} in the database...')
            match_df = pd.DataFrame(list(self.mongo[f'{val_player_obj.puuid}'].comp_rr_history.find()), columns=['match_id', 'map', 'mmr_change', 'date', 'rank', 'current_mmr', 'rank_image_url']).sort_values('date', ascending=False)
            if should_update_match_history:
                print(f'Attempting to update match history for {player}...')
                account_mmr_history_url = f'https://api.henrikdev.xyz/valorant/v1/by-puuid/mmr-history/{val_player_obj.region}/{val_player_obj.puuid}'
                async with request('GET', account_mmr_history_url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        data = data['data']

                        if len(data) < 1:
                            await interaction.followup.send(f'{player} has no recently logged ranked games')
                            return

                        new_mmr_changes, new_match_maps, new_match_ids, new_match_times = [], [], [], []
                        new_account_rank_history, new_account_rr_history, new_account_rank_url_history = [], [], []
                        
                        count = 0
                        max_count = min(10, len(data))
                        while count < max_count:
                            match = data[count]
                            match_id = match['match_id']
                            mmr_change = match['mmr_change_to_last_game']
                            match_map = match['map']['name']
                            account_rank = match['currenttierpatched']
                            account_rr = match['ranking_in_tier']
                            account_rank_url = match['images']['small']
                            date = int(match['date_raw'])

                            new_match_ids.append(match_id)
                            new_match_maps.append(match_map)
                            new_mmr_changes.append(int(mmr_change))
                            new_match_times.append(datetime.fromtimestamp(date))
                            new_account_rank_history.append(account_rank)
                            new_account_rr_history.append(account_rr)
                            new_account_rank_url_history.append(account_rank_url)
                            count += 1
                        new_match_df = pd.DataFrame({'match_id': new_match_ids, 'map': new_match_maps, 'mmr_change': new_mmr_changes, 'date': new_match_times, 'rank': new_account_rank_history, 'current_mmr': new_account_rr_history, 'rank_image_url': new_account_rank_url_history})
                        # get duplicate match ids
                        existing_match_ids = set(match_df['match_id'])
                        new_unique_match_ids = [match_id for match_id in new_match_ids if match_id not in existing_match_ids]

                        # filter new_match_df to keep only unique rows
                        unique_new_match_df = new_match_df[new_match_df['match_id'].isin(new_unique_match_ids)]

                        # insert unique rows
                        if not unique_new_match_df.empty:
                            self.mongo[f'{val_player_obj.puuid}'].comp_rr_history.insert_many(unique_new_match_df.to_dict('records'))

                        match_df = pd.DataFrame(list(self.mongo[f'{val_player_obj.puuid}'].comp_rr_history.find()), columns=['match_id', 'map', 'mmr_change', 'date', 'rank', 'current_mmr', 'rank_image_url']).sort_values('date', ascending=False)

            match_ids = list(match_df['match_id'])
            match_maps = list(match_df['map'])
            mmr_changes = list(match_df['mmr_change'])
            match_times = list(match_df['date'])
            account_rank_history = list(match_df['rank'])
            account_rr_history = list(match_df['current_mmr'])
            account_rank_url_history = list(match_df['rank_image_url'])
            max_count = min(10, len(match_ids))
            for i in range(max_count):
                match_info.append(f'{mmr_changes[i]} ({match_maps[i]})')
            
        else:
            # else add to database
            print(f'Getting match history for {player} from the API...')
            account_mmr_history_url = f'https://api.henrikdev.xyz/valorant/v1/by-puuid/mmr-history/{val_player_obj.region}/{val_player_obj.puuid}'
            async with request('GET', account_mmr_history_url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    data = data['data']

                    if len(data) < 1:
                        await interaction.followup.send(f'{player} has no recently logged ranked games')
                        return
                    
                    count = 0
                    max_count = min(10, len(data))
                    while count < max_count:
                        match = data[count]
                        match_id = match['match_id']
                        mmr_change = match['mmr_change_to_last_game']
                        match_map = match['map']['name']
                        account_rank = match['currenttierpatched']
                        account_rr = match['ranking_in_tier']
                        account_rank_url = match['images']['small']
                        date = int(match['date_raw'])

                        match_ids.append(match_id)
                        match_maps.append(match_map)
                        mmr_changes.append(int(mmr_change))
                        match_times.append(datetime.fromtimestamp(date))
                        account_rank_history.append(account_rank)
                        account_rr_history.append(account_rr)
                        account_rank_url_history.append(account_rank_url)
                        match_info.append(f'{mmr_change} ({match_map})')
                        count += 1

                    # get match df and upsert (update and insert) into mongodb
                    match_df = pd.DataFrame({'match_id': match_ids, 'map': match_maps, 'mmr_change': mmr_changes, 'date': match_times, 'rank': account_rank_history, 'current_mmr': account_rr_history, 'rank_image_url': account_rank_url_history})
                    self.mongo[f'{val_player_obj.puuid}'].comp_rr_history.insert_many(match_df.to_dict("records"))
 
        # get current rank
        account_rank = account_rank_history[0]
        account_rr = account_rr_history[0]
        account_rank_url = account_rank_url_history[0]

        # Test embed
        embed=discord.Embed(description=f'', color=0x36ecc8)
        embed.add_field(name='Level', value=val_player_obj.level, inline=True)
        if val_player_obj.has_title:
            player_title = await val_player_obj.getTitle()
            if player_title != None:
                embed.add_field(name='Title', value=player_title, inline=True)
        
        embed.add_field(name='Rank', value=f'{account_rank} ({account_rr} / 100 RR)', inline=False)
        embed.add_field(name=f'RR history ({max_count} Games)',  value=", ".join(match_info), inline=False)
        embed.add_field(name=f'RR Gain', value=sum(mmr_changes[:max_count]), inline=True)
        embed.add_field(name=f'Average RR Gain', value=round(sum(mmr_changes[:max_count])/len(mmr_changes[:max_count]), 2), inline=True)
        
        if account_rank_url:
            embed.set_author(name=f'{player} ({val_player_obj.region})', icon_url=f'{account_rank_url}')
        else:
            embed.set_author(name=f'{player} ({val_player_obj.region})')
        
        if val_player_obj.has_card:
            embed.set_thumbnail(url=f'{val_player_obj.getCardPfpUrl()}')

        await interaction.followup.send(embed=embed)

    @app_commands.slash_command(name='comp_history', description='Get a valorant players competitive history')
    async def comp_history(self, interaction: discord.Interaction, player):
        await interaction.response.defer()

        unfiltered_player_name = player.split('#')
        if len(unfiltered_player_name) != 2:
            await interaction.followup.send(f'Error: {player} is not a valid player name!')
            return

        player_name = unfiltered_player_name[0]
        player_tag = unfiltered_player_name[1]
        player_uuid = None
        should_update_match_history = False

        player_df = pd.DataFrame(list(self.playerdb.Players.find()), columns=['player', 'puuid', 'last_updated', 'region'])
        matches_df = pd.DataFrame(list(self.matchesdb.Match_Data.find()), columns=['map_name', 'server', 'match_id', 'blue_score', 'red_score', 'who_won', 'match_players', 'start_date', 'end_date'])
        player_df.astype(str)

        try:
            player_uuid = player_df.loc[player_df.player.str.lower() == str(player).lower(), 'puuid'].values[0]
            player_region = player_df.loc[player_df.player.str.lower() == str(player).lower(), 'region'].values[0]
            delta = datetime.now() - datetime.strptime(player_df.loc[(player_df['puuid'] == player_uuid), 'last_updated'].values[0], "%Y-%m-%dT%H:%M:%S.%fZ")
            if abs(delta.total_seconds()) > (5 * 60):
                should_update_match_history = True
        except (KeyError, IndexError):
            await interaction.followup.send(f'{player} not in local database! Check that you have the correct tag, or do "/val_stats {player}" before running this command again.')
            return

        if f'{player_uuid}' in self.mongo.list_database_names():
            print(f'Getting match history for {player} from the database...')
            print(f'REGION: {player_region}')
            match_df = pd.DataFrame(list(self.mongo[f'{player_uuid}'].comp_rr_history.find()), columns=['match_id', 'map', 'mmr_change', 'date', 'rank', 'current_mmr', 'rank_image_url']).sort_values('date', ascending=False)
            if should_update_match_history:
                print(f'Attempting to update match history for {player}...')
                account_mmr_history_url = f'https://api.henrikdev.xyz/valorant/v1/by-puuid/mmr-history/{player_region}/{player_uuid}'
                async with request('GET', account_mmr_history_url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        data = data['data']

                        if len(data) < 1:
                            await interaction.followup.send(f'{player} has no recently logged ranked games')
                            return

                        new_mmr_changes, new_match_maps, new_match_ids, new_match_times = [], [], [], []
                        new_account_rank_history, new_account_rr_history, new_account_rank_url_history = [], [], []
                        
                        count = 0
                        max_count = min(10, len(data))
                        while count < max_count:
                            match = data[count]
                            match_id = match['match_id']
                            mmr_change = match['mmr_change_to_last_game']
                            match_map = match['map']['name']
                            account_rank = match['currenttierpatched']
                            account_rr = match['ranking_in_tier']
                            account_rank_url = match['images']['small']
                            date = int(match['date_raw'])

                            new_match_ids.append(match_id)
                            new_match_maps.append(match_map)
                            new_mmr_changes.append(int(mmr_change))
                            new_match_times.append(datetime.fromtimestamp(date))
                            new_account_rank_history.append(account_rank)
                            new_account_rr_history.append(account_rr)
                            new_account_rank_url_history.append(account_rank_url)
                            count += 1
                        new_match_df = pd.DataFrame({'match_id': new_match_ids, 'map': new_match_maps, 'mmr_change': new_mmr_changes, 'date': new_match_times, 'rank': new_account_rank_history, 'current_mmr': new_account_rr_history, 'rank_image_url': new_account_rank_url_history})
                        # get duplicate match ids
                        existing_match_ids = set(match_df['match_id'])
                        new_unique_match_ids = [match_id for match_id in new_match_ids if match_id not in existing_match_ids]

                        # filter new_match_df to keep only unique rows
                        unique_new_match_df = new_match_df[new_match_df['match_id'].isin(new_unique_match_ids)]

                        # insert unique rows
                        if not unique_new_match_df.empty:
                            self.mongo[f'{player_uuid}'].comp_rr_history.insert_many(unique_new_match_df.to_dict('records'))

                        match_df = pd.DataFrame(list(self.mongo[f'{player_uuid}'].comp_rr_history.find()), columns=['match_id', 'map', 'mmr_change', 'date', 'rank', 'current_mmr', 'rank_image_url']).sort_values('date', ascending=False)
                    else:
                        print(f'ERROR: REPSONSE STATUS {response.status}')
            match_ids = list(match_df['match_id'])
            mmr_changes = list(match_df['mmr_change'])
            max_count = min(5, len(match_ids)) # Get first 5 match ids
            formatted_match_strings = []
            if max_count == 0:
                await interaction.followup.send(f'{player} has not played a competitive match yet!')
                return
            
            for i in range(max_count):
                if matches_df['match_id'].str.contains(match_ids[i]).any():
                    print(f'Getting match data for {match_ids[i]} in the database...')
                    
                    match_row = matches_df[matches_df['match_id'] == match_ids[i]]
                    match_data = match_row.iloc[0]
                    lookup_team = None
                    match_players = []
                    for player_data in match_data["match_players"]:
                        match_player_obj = match_player(player_data["name"], player_data["tag"], player_data["team_id"], player_data["agent"], player_data["kills"], player_data["deaths"], player_data["score"], player_data["assists"], player_data["headshots"], player_data["bodyshots"], player_data["legshots"], player_data["ability_casts"]["grenade"], player_data["ability_casts"]["ability1"], player_data["ability_casts"]["ability2"], player_data["ability_casts"]["ultimate"], player_data["tier"])
                        match_players.append(match_player_obj)
                        if player_name.lower() == match_player_obj.player_name.lower() and player_tag.lower() == match_player_obj.player_tag.lower():
                            lookup_team = player_data['team_id']
                    match_obj = comp_match(match_data['map_name'], match_data['server'], match_data['match_id'], match_data['blue_score'], match_data['red_score'], match_data['who_won'], match_players, lookup_team, player, match_data['start_date'], match_data['end_date'])
                    formatted_match_strings.append(match_obj.get_formatted_map())
                else:
                    print(f'Getting match data for {match_ids[i]} in the API...')
                    async with request('GET', f'https://api.henrikdev.xyz/valorant/v4/match/{player_region.lower()}/{match_ids[i]}', headers=headers) as response:
                        if response.status == 200:
                            data = await response.json()
                            data = data['data']
                            metadata = data['metadata']
                            players = data['players']
                            teams = data['teams']

                            lookup_team = None
                            match_players = []
                            blue_score = 0
                            red_score = 0
                            who_won = None
                            match_start_time = datetime.strptime(metadata['started_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
                            match_finish_time = match_start_time + timedelta(milliseconds=int(metadata['game_length_in_ms']))

                            # print(f"Map: {metadata['map']['name']}, Server: {metadata['cluster']}, Match ID: {metadata['match_id']}")
                            for player_index in range(len(players)):
                                player_data = players[player_index]
                                player_stats = player_data['stats']
                                ability_casts = player_data['ability_casts']
                                match_player_obj = match_player(player_data['name'], player_data['tag'], player_data['team_id'], player_data['agent']['name'].lower(), player_stats['kills'], player_stats['deaths'], player_stats['score'], player_stats['assists'], player_stats['headshots'], player_stats['bodyshots'], player_stats['legshots'], ability_casts['grenade'], ability_casts['ability1'], ability_casts['ability2'], ability_casts['ultimate'], player_data['tier']['name'])
                                if player_name.lower() == player_data['name'].lower() and player_tag.lower() == player_data['tag'].lower():
                                    lookup_team = player_data['team_id']
                                match_players.append(match_player_obj)

                            for team_index in range(len(teams)):
                                team_data = teams[team_index]
                                if team_data['team_id'] == 'Red':
                                    red_score = team_data['rounds']['won']
                                    if team_data['won'] == True:
                                        who_won = 'Red'
                                elif team_data['team_id'] == 'Blue':
                                    blue_score = team_data['rounds']['won']
                                    if team_data['won'] == True:
                                        who_won = 'Blue'
                            if who_won == None:
                                who_won = 'Tie'
                            
                            match_obj = comp_match(metadata['map']['name'], metadata['cluster'], metadata['match_id'], blue_score, red_score, who_won, match_players, lookup_team, player, match_start_time, match_finish_time)
                            formatted_match_strings.append(match_obj.get_formatted_map())
                            
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
                                "match_players": player_data_list,
                                "start_date": match_obj.start_date,
                                "end_date": match_obj.end_date,
                            }
                            
                            self.matchesdb['Match_Data'].insert_one(match_data)
                        else:
                            print(f'ERROR: REPSONSE STATUS {response.status} for https://api.henrikdev.xyz/valorant/v4/match{player_region.lower()}/{match_ids[i]}')

            max_count = len(formatted_match_strings)
            embed=discord.Embed(description='', color=0x3c88eb)
            embed.set_author(name=f'{player} | LAST {max_count} GAMES')
            print(len(formatted_match_strings), max_count)
            for i in range(max_count):
                # print(formatted_match_strings[i])
                try:
                    embed.add_field(name=formatted_match_strings[i][0] + f' ({get_mmr_change_emoji(mmr_changes[i])} {mmr_changes[i]} RR)', value=formatted_match_strings[i][1], inline=False)
                except Exception as e:
                    print(e)
            await interaction.followup.send(embed=embed)

        else:
            await interaction.followup.send(f'{player} not in local database, please run "/val_player {player}" again to load the player again!')
            return

class val_player():

    def __init__(self, puuid, player_name, player_tag, region, level, title_id, card_id):
        self.puuid = str(puuid)
        self.player_name = str(player_name)
        self.player_tag = str(player_tag)
        self.region = str(region)
        self.level = int(level)
        self.has_title = title_id != None
        self.title_id = str(title_id)
        self.has_card = card_id != None
        self.card_id = str(card_id)

    # Get the player's card (small/pfp) image if it exists
    def getCardPfpUrl(self) -> str:
        if self.has_card:
            return f'https://media.valorant-api.com/playercards/{self.card_id}/smallart.png'
        else:
            return None
        
    # Get the player's title if it exists
    async def getTitle(self) -> str:
        if self.has_title:
            title_link_url = f'https://valorant-api.com/v1/playertitles/{self.title_id}'
            async with request('GET', title_link_url, headers={}) as response:
                if response.status == 200:
                    data = await response.json()
                    data = data['data']
                    return data['titleText']
                else:
                    return None
        else:
            return None
    
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
        if e_ability != None:
            self.e_ability = int(e_ability)
        else:
            self.e_ability = 0
        if c_ability != None:
            self.c_ability = int(c_ability)
        else:
            self.c_ability = 0
        if q_ability != None:
            self.q_ability = int(q_ability)
        else:
            self.q_ability = 0
        if x_ability != None:
            self.x_ability = int(x_ability)
        else:
            self.x_ability = 0
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
        