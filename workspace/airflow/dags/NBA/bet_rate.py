def bet_rate():
    import requests
    from datetime import datetime, timedelta
    from tqdm import tqdm
    import pandas as pd
    import json
    from concurrent.futures import ThreadPoolExecutor
    from threading import Lock
    write_lock = Lock()

    list_url = [f"https://api.nflpickwatch.com/v1/general/games/2023/{i}/nba" for i in range(1, 400)]
    headers = {
    'authority': 'api.nflpickwatch.com',
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en,vi;q=0.9',
    'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MTEwNjY0LCJjbGFpbXMiOlsicCJdLCJpYXQiOjE2OTU2MzExNzEsImV4cCI6MTcxMTQ0MjM3MX0.6mpM8uRxTwwWOkQDCEF51eMkvEe34hfazP3YsUWKnYM',
    'origin': 'https://nflpickwatch.com',
    'referer': 'https://nflpickwatch.com/',
    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    }
    code_team = {
        'ATL': 'Atlanta Hawks',
        'BKN': 'Brooklyn Nets',
        'BOS': 'Boston Celtics',
        'CHA': 'Charlotte Hornets',
        'CHI': 'Chicago Bulls',
        'CLE': 'Cleveland Cavaliers',
        'DAL': 'Dallas Mavericks',
        'DEN': 'Denver Nuggets',
        'DET': 'Detroit Pistons',
        'GS': 'Golden State Warriors',
        'HOU': 'Houston Rockets',
        'IND': 'Indiana Pacers',
        'LAC': 'LA Clippers',
        'LAL': 'Los Angeles Lakers',
        'MEM': 'Memphis Grizzlies',
        'MIA': 'Miami Heat',
        'MIL': 'Milwaukee Bucks',
        'MIN': 'Minnesota Timberwolves',
        'NO': 'New Orleans Pelicans',
        'NY': 'New York Knicks',
        'OKC': 'Oklahoma City Thunder',
        'ORL': 'Orlando Magic',
        'PHI': 'Philadelphia 76ers',
        'PHO': 'Phoenix Suns',
        'POR': 'Portland Trail Blazers',
        'SA': 'San Antonio Spurs',
        'SAC': 'Sacramento Kings',
        'TOR': 'Toronto Raptors',
        'UTA': 'Utah Jazz',
        'WAS': 'Washington Wizards'
    }

    teams_dict = {
        'Atlanta Hawks': 1610612737,
        'Boston Celtics': 1610612738,
        'Brooklyn Nets': 1610612751,
        'Charlotte Hornets': 1610612766,
        'Chicago Bulls': 1610612741,
        'Cleveland Cavaliers': 1610612739,
        'Dallas Mavericks': 1610612742,
        'Denver Nuggets': 1610612743,
        'Detroit Pistons': 1610612765,
        'Golden State Warriors': 1610612744,
        'Houston Rockets': 1610612745,
        'Indiana Pacers': 1610612754,
        'LA Clippers': 1610612746,
        'Los Angeles Lakers': 1610612747,
        'Memphis Grizzlies': 1610612763,
        'Miami Heat': 1610612748,
        'Milwaukee Bucks': 1610612749,
        'Minnesota Timberwolves': 1610612750,
        'New Orleans Pelicans': 1610612740,
        'New York Knicks': 1610612752,
        'Oklahoma City Thunder': 1610612760,
        'Orlando Magic': 1610612753,
        'Philadelphia 76ers': 1610612755,
        'Phoenix Suns': 1610612756,
        'Portland Trail Blazers': 1610612757,
        'Sacramento Kings': 1610612758,
        'San Antonio Spurs': 1610612759,
        'Toronto Raptors': 1610612761,
        'Utah Jazz': 1610612762,
        'Washington Wizards': 1610612764
    }

    def get_data(url):
        response = requests.get(url,headers=headers)
        try:
            target = response.json()
            for i in target:
                data = dict()
                # data["id"] = str(i["additional_data"].get("GameID"))
                data["date"] = str(i["additional_data"].get("DateTime").split("T")[0].split("-")[0] + "-" + i["additional_data"].get("DateTime").split("T")[0].split("-")[1] + "-" + i["additional_data"].get("DateTime").split("T")[0].split("-")[2])
                data["team_1_id"] = int(teams_dict.get(code_team.get(i["additional_data"].get("AwayTeam"),i["additional_data"].get("AwayTeam")),code_team.get(i["additional_data"].get("AwayTeam"),i["additional_data"].get("AwayTeam"))))
                data["team_1_name"] = str(code_team.get(i["additional_data"].get("AwayTeam"),i["additional_data"].get("AwayTeam")))
                data["team_2_id"] = int(teams_dict.get(code_team.get(i["additional_data"].get("HomeTeam"),i["additional_data"].get("HomeTeam")),code_team.get(i["additional_data"].get("HomeTeam"),i["additional_data"].get("HomeTeam"))))
                data["team_2_name"] = str(code_team.get(i["additional_data"].get("HomeTeam"),i["additional_data"].get("HomeTeam")))
                data["team_1_score"] = int(i["additional_data"].get("AwayTeamScore"))
                data["team_2_score"] = int(i["additional_data"].get("HomeTeamScore"))
                data["spread"] = float(i["additional_data"].get("PointSpread"))
                data["ov_value"] = float(i["additional_data"].get("OverUnder"))
                # lưu file ha
                with write_lock:
                    df = pd.DataFrame([data])
                    df.to_csv("./data/betrate.csv", mode='a', header=not pd.io.common.file_exists("./data/betrate.csv"), index=False)
        except Exception as e:
            print(e)
            pass
        
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(get_data, list_url)
        
        
        
def boxscores_advanced():        
    import requests
    from datetime import datetime, timedelta
    from concurrent.futures import ThreadPoolExecutor
    from tqdm import tqdm
    import pandas as pd
    from threading import Lock
    write_lock = Lock()


    headers = {
        'authority': 'core-api.nba.com',
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'ocp-apim-subscription-key': '747fa6900c6c4e89a58b81b72f36eb96',
        'origin': 'https://www.nba.com',
        'pragma': 'no-cache',
        'referer': 'https://www.nba.com/',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
    }
    headerss = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Origin': 'https://www.nba.com',
        'Referer': 'https://www.nba.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }


    def check_requests(url, HEADERS):
        check_out = 3
        res = None
        while check_out > 0:
            res = requests.get(url, headers=HEADERS)
            if res.status_code == 200: 
                break
            check_out -= 1
        return res


    def split_periods(period):
        return period.split('&')[-2].replace('startPeriod=','')
    def split_day (day):
        return day.split('T')[0]

        
    # Ngày hiện tại
    current_time = datetime.now().date()- timedelta(days=1)
    previous_day = current_time - timedelta(days=2)



    start_date = datetime(int(str(previous_day).split("-")[0]), int(str(previous_day).split("-")[1]), int(str(previous_day).split("-")[2])).date()
    end_date = datetime(int(str(current_time).split("-")[0]), int(str(current_time).split("-")[1]), int(str(current_time).split("-")[2])).date()

    # start_date = datetime(2023, 10, 25).date()
    # end_date = datetime(2023, 12, 17).date()
    num_days = (end_date - start_date).days + 1  
    extended_dates = []
    for day in tqdm(range(num_days)):
        current_date = start_date + timedelta(days=day)
        extended_dates.append("https://core-api.nba.com/cp/api/v1.3/feeds/gamecardfeed?gamedate="+str(current_date.strftime("%m/%d/%Y")) +"&platform=web")



    def process_url(url):

        try:
            response = check_requests(url, headers)
            if response:
                datas = response.json()

                for i in datas['modules'][0]['cards']:
                    game_id = i['cardData']['gameId']
                    day = i['cardData']['gameTimeEastern']
                    # comment
                    for period in range(1, 5):
                        url = f"https://stats.nba.com/stats/boxScoreAdvancedv3?GameID={game_id}&LeagueID=00&endPeriod={period}&endRange=28800&rangeType=1&startPeriod={period}&startRange=0"
                        response = check_requests(url, headerss)
                        if response:
                            datas = response.json()
                            periods = split_periods(url)
                            gameid = datas['boxScoreAdvanced']['gameId']
                            away_team_id = datas['boxScoreAdvanced']['awayTeamId']
                            home_team_id = datas['boxScoreAdvanced']['homeTeamId']
                            all_players = datas['boxScoreAdvanced']['awayTeam']['players'] + datas['boxScoreAdvanced']['homeTeam']['players']
                            team_types = ['awayTeam'] * len(datas['boxScoreAdvanced']['awayTeam']['players']) + \
                                        ['homeTeam'] * len(datas['boxScoreAdvanced']['homeTeam']['players'])

                            for player, team_type in zip(all_players, team_types):
                                data = dict()
                                time = datas['meta'].get('time')
                                data['day'] = split_day(day)
                                data['game_id'] = int(gameid)
                                data['team_name'] = datas['boxScoreAdvanced'][team_type]['teamCity'] + ' ' + datas['boxScoreAdvanced'][team_type]['teamName']
                                data['player_name'] = player['firstName'] + ' ' + player['familyName']
                                data['periods'] = "Q" + str(periods)
                                # data['min'] = (player['statistics'].get('minutes'))
                                data['min'] = (player['statistics'].get('minutes'))
                                data['offrtg'] = round(float(player['statistics'].get('offensiveRating')), 2)
                                data['defrtg'] = round(float(player['statistics'].get('defensiveRating')),2)
                                data['netrtg'] = round(float(player['statistics'].get('netRating')),2)
                                data['ast_percent'] = round(float(player['statistics'].get('assistPercentage'))*100,2)
                                data['ast_to'] = round(float(player['statistics'].get('assistToTurnover')),2)
                                data['ast_ratio'] = round(float(player['statistics'].get('assistRatio')),2)
                                data['oreb_percent'] = round(float(player['statistics'].get('offensiveReboundPercentage'))*100,2)
                                data['dreb_percent'] = round(float(player['statistics'].get('defensiveReboundPercentage'))*100,2)
                                data['reb_percent'] = round(float(player['statistics'].get('reboundPercentage'))*100,2)
                                data['to_ratio'] = round(float(player['statistics'].get('turnoverRatio')),2)
                                data['efg_percent'] = round(float(player['statistics'].get('effectiveFieldGoalPercentage'))*100,2)
                                data['ts_percent'] = round(float(player['statistics'].get('trueShootingPercentage'))*100,2)
                                data['usg_percent'] = round(float(player['statistics'].get('usagePercentage'))*100,2)
                                data['pace'] = round(float(player['statistics'].get('pace')),2)
                                data['pie'] = round(float(player['statistics'].get('PIE'))*100,2)
                                data['position'] = player['position']
                                data['team_id'] = int(away_team_id if team_type == 'awayTeam' else home_team_id)
                                with write_lock:
                                    df = pd.DataFrame([data])
                                    df.to_csv("./data/boxscores_advanced.csv", mode='a', header=not pd.io.common.file_exists("./data/boxscores_advanced.csv"), index=False)
                                    
        except Exception as e:
            print(f"An error occurred: {e}")

    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(process_url, extended_dates)
    
    
    
def boxscores_defense():
    import requests
    from datetime import datetime, timedelta
    from concurrent.futures import ThreadPoolExecutor
    from tqdm import tqdm
    import pandas as pd
    from threading import Lock
    write_lock = Lock()


    headers = {
        'authority': 'core-api.nba.com',
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'ocp-apim-subscription-key': '747fa6900c6c4e89a58b81b72f36eb96',
        'origin': 'https://www.nba.com',
        'pragma': 'no-cache',
        'referer': 'https://www.nba.com/',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
    }
    headerss = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Origin': 'https://www.nba.com',
        'Referer': 'https://www.nba.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }






    def check_requests(url, HEADERS=None):
        check_out = 3
        res = None
        while check_out > 0:
            res = requests.get(url, headers=HEADERS)
            if res.status_code == 200: 
                break
            check_out -= 1
        return res


    def split_periods(period):
        return period.split('&')[-2].replace('startPeriod=','')
    def split_day (day):
        return day.split('T')[0]

        
    # Ngày hiện tại
    current_time = datetime.now().date()- timedelta(days=1)
    previous_day = current_time - timedelta(days=2)

    # start_date = datetime(2023, 10, 25).date()


    start_date = datetime(int(str(previous_day).split("-")[0]), int(str(previous_day).split("-")[1]), int(str(previous_day).split("-")[2])).date()
    end_date = datetime(int(str(current_time).split("-")[0]), int(str(current_time).split("-")[1]), int(str(current_time).split("-")[2])).date()
    # end_date = datetime(2021, 11, 17)
    # start_date = datetime(2023, 10, 25).date()
    # end_date = datetime(2023, 12, 17).date()
    num_days = (end_date - start_date).days + 1  
    extended_dates = []
    for day in tqdm(range(num_days)):
        current_date = start_date + timedelta(days=day)
        extended_dates.append("https://core-api.nba.com/cp/api/v1.3/feeds/gamecardfeed?gamedate="+str(current_date.strftime("%m/%d/%Y")) +"&platform=web")

    def process_url(url):

        try:
            response = check_requests(url, headers)
            if response:
                datas = response.json()

                for i in datas['modules'][0]['cards']:
                    game_id = i['cardData']['gameId']
                    day = i['cardData']['gameTimeEastern']
                    # comment
                    for period in range(1, 2):
                        url = f"https://stats.nba.com/stats/boxscoredefensivev2?GameID={game_id}&LeagueID=00&endPeriod={period}&endRange=28800&rangeType=1&startPeriod={period}&startRange=0"
                        response = check_requests(url, headerss)
                        if response:
                            datas = response.json()
                            periods = split_periods(url)
                            gameid = datas['boxScoreDefensive']['gameId']
                            away_team_id = datas['boxScoreDefensive']['awayTeamId']
                            home_team_id = datas['boxScoreDefensive']['homeTeamId']
                            all_players = datas['boxScoreDefensive']['awayTeam']['players'] + datas['boxScoreDefensive']['homeTeam']['players']
                            team_types = ['awayTeam'] * len(datas['boxScoreDefensive']['awayTeam']['players']) + \
                                        ['homeTeam'] * len(datas['boxScoreDefensive']['homeTeam']['players'])

                            for player, team_type in zip(all_players, team_types):
                                data = dict()
                                time = datas['meta'].get('time')
                                data['day'] = split_day(day)
                                data['game_id'] = int(gameid)
                                data['team_name'] = datas['boxScoreDefensive'][team_type]['teamCity'] + ' ' + datas['boxScoreDefensive'][team_type]['teamName']
                                data['player_name'] = player['firstName'] + ' ' + player['familyName']
                                # data['periods'] = "Q" + str(periods)
                                data['def_min'] = (player['statistics'].get('matchupMinutes'))
                                data['partial_poss'] = round(float(player['statistics'].get('partialPossessions')),2)
                                data['pts'] = round(float(player['statistics'].get('playerPoints')),2)
                                data['dreb'] = round(float(player['statistics'].get('defensiveRebounds')),2)
                                data['ast'] = round(float(player['statistics'].get('matchupAssists')),2)
                                data['tov'] = round(float(player['statistics'].get('matchupTurnovers')),2)
                                data['stl'] = round(float(player['statistics'].get('steals')),2)
                                data['blk'] = round(float(player['statistics'].get('blocks')),2)
                                data['dfgm'] = round(float(player['statistics'].get('matchupFieldGoalsMade')),2)
                                data['dfga'] = round(float(player['statistics'].get('matchupFieldGoalsAttempted')),2)
                                data['dfg_percent'] = round(float(player['statistics'].get('matchupFieldGoalPercentage'))*100,2)
                                data['d3pm'] = round(float(player['statistics'].get('matchupThreePointersMade')),2)
                                data['d3pa'] = round(float(player['statistics'].get('matchupThreePointersAttempted')),2)
                                data['d3p_percent'] = round(float(player['statistics'].get('matchupThreePointerPercentage'))*100,2)
                                data['position'] = player['position']
                                data['team_id'] = int(away_team_id if team_type == 'awayTeam' else home_team_id)
                                with write_lock:
                                    df = pd.DataFrame([data])
                                    df.to_csv("./data/boxscores_defense.csv", mode='a', header=not pd.io.common.file_exists("./data/boxscores_defense.csv"), index=False)
        except IndexError as e:
            print(f"Index error occurred: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(process_url, extended_dates)






def boxscores_fourfactors():

    import requests
    from datetime import datetime, timedelta
    from concurrent.futures import ThreadPoolExecutor
    from tqdm import tqdm
    import pandas as pd
    from threading import Lock
    write_lock = Lock()
    


    headers = {
        'authority': 'core-api.nba.com',
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'ocp-apim-subscription-key': '747fa6900c6c4e89a58b81b72f36eb96',
        'origin': 'https://www.nba.com',
        'pragma': 'no-cache',
        'referer': 'https://www.nba.com/',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
    }
    headerss = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Origin': 'https://www.nba.com',
        'Referer': 'https://www.nba.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }






    def check_requests(url, HEADERS=None):
        check_out = 3
        res = None
        while check_out > 0:
            res = requests.get(url, headers=HEADERS)
            if res.status_code == 200: 
                break
            check_out -= 1
        return res


    def split_periods(period):
        return period.split('&')[-2].replace('startPeriod=','')
    def split_day (day):
        return day.split('T')[0]

        
    # Ngày hiện tại
    current_time = datetime.now().date()- timedelta(days=1)
    previous_day = current_time - timedelta(days=2)

    # start_date = datetime(2023, 10, 25).date()


    start_date = datetime(int(str(previous_day).split("-")[0]), int(str(previous_day).split("-")[1]), int(str(previous_day).split("-")[2])).date()
    end_date = datetime(int(str(current_time).split("-")[0]), int(str(current_time).split("-")[1]), int(str(current_time).split("-")[2])).date()
    # end_date = datetime(2021, 11, 17)
    # start_date = datetime(2023, 10, 25).date()
    # end_date = datetime(2023, 12, 17).date()
    num_days = (end_date - start_date).days + 1  
    extended_dates = []
    for day in tqdm(range(num_days)):
        current_date = start_date + timedelta(days=day)
        extended_dates.append("https://core-api.nba.com/cp/api/v1.3/feeds/gamecardfeed?gamedate="+str(current_date.strftime("%m/%d/%Y")) +"&platform=web")



    def process_url(url):

        try:
            response = check_requests(url, headers)
            if response:
                datas = response.json()  

                for i in datas['modules'][0]['cards']:
                    game_id = i['cardData']['gameId']
                    day = i['cardData']['gameTimeEastern']
                    # comment
                    for period in range(1, 5):
                        url = f"https://stats.nba.com/stats/boxscorefourfactorsv3?GameID={game_id}&LeagueID=00&endPeriod={period}&endRange=28800&rangeType=1&startPeriod={period}&startRange=0"
                        response = check_requests(url, headerss)
                        if response:
                            datas = response.json()
                            periods = split_periods(url)
                            gameid = datas['boxScoreFourFactors']['gameId']
                            away_team_id = datas['boxScoreFourFactors']['awayTeamId']
                            home_team_id = datas['boxScoreFourFactors']['homeTeamId']
                            all_players = datas['boxScoreFourFactors']['awayTeam']['players'] + datas['boxScoreFourFactors']['homeTeam']['players']
                            team_types = ['awayTeam'] * len(datas['boxScoreFourFactors']['awayTeam']['players']) + \
                                        ['homeTeam'] * len(datas['boxScoreFourFactors']['homeTeam']['players'])

                            for player, team_type in zip(all_players, team_types):
                                data = dict()
                                time = datas['meta'].get('time')
                                data['day'] = split_day(day)
                                data['game_id'] = int(gameid)
                                data['team_name'] = datas['boxScoreFourFactors'][team_type]['teamCity'] + ' ' + datas['boxScoreFourFactors'][team_type]['teamName']
                                data['player_name'] = player['firstName'] + ' ' + player['familyName']
                                data['periods'] = "Q" + str(periods)
                                # data['min'] = (player['statistics'].get('minutes'))
                                data['min'] = (player['statistics'].get('minutes'))
                                data['efg_percent'] = round(float(player['statistics'].get('effectiveFieldGoalPercentage'))*100, 2)
                                data['fta_rate'] = round(float(player['statistics'].get('freeThrowAttemptRate')),2)
                                data['tm_to_percent'] = round(float(player['statistics'].get('teamTurnoverPercentage'))*100,2)
                                data['oreb_percent'] = round(float(player['statistics'].get('offensiveReboundPercentage'))*100,2)
                                data['opp_efg_percent'] = round(float(player['statistics'].get('oppEffectiveFieldGoalPercentage'))*100,2)
                                data['opp_fta_rate'] = round(float(player['statistics'].get('oppFreeThrowAttemptRate')),2)
                                data['opp_to_percent'] = round(float(player['statistics'].get('oppTeamTurnoverPercentage'))*100,2)
                                data['opp_oreb_percent'] = round(float(player['statistics'].get('oppOffensiveReboundPercentage'))*100,2)
                                data['position'] = player['position']
                                data['team_id'] = int(away_team_id if team_type == 'awayTeam' else home_team_id)
                                with write_lock:
                                    df = pd.DataFrame([data])
                                    df.to_csv("./data/boxscores_fourfactors.csv", mode='a', header=not pd.io.common.file_exists("./data/boxscores_fourfactors.csv"), index=False)

        except Exception as e:
            print(f"An error occurred: {e}")

    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(process_url, extended_dates)



def boxscores_hustle():



    import requests
    from datetime import datetime, timedelta
    from concurrent.futures import ThreadPoolExecutor
    from tqdm import tqdm
    import pandas as pd
    from threading import Lock  
    write_lock = Lock() 


    headers = {
        'authority': 'core-api.nba.com',
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'ocp-apim-subscription-key': '747fa6900c6c4e89a58b81b72f36eb96',
        'origin': 'https://www.nba.com',
        'pragma': 'no-cache',
        'referer': 'https://www.nba.com/',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
    }
    headerss = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Origin': 'https://www.nba.com',
        'Referer': 'https://www.nba.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }






    def check_requests(url, HEADERS=None):
        check_out = 3
        res = None
        while check_out > 0:
            res = requests.get(url, headers=HEADERS)
            if res.status_code == 200: 
                break
            check_out -= 1
        return res


    def split_periods(period):
        return period.split('&')[-2].replace('startPeriod=','')
    def split_day (day):
        return day.split('T')[0]

        
    # Ngày hiện tại
    current_time = datetime.now().date()- timedelta(days=1)
    previous_day = current_time - timedelta(days=2)

    # start_date = datetime(2023, 10, 25).date()


    start_date = datetime(int(str(previous_day).split("-")[0]), int(str(previous_day).split("-")[1]), int(str(previous_day).split("-")[2])).date()
    end_date = datetime(int(str(current_time).split("-")[0]), int(str(current_time).split("-")[1]), int(str(current_time).split("-")[2])).date()
    # end_date = datetime(2021, 11, 17)
    # start_date = datetime(2023, 10, 25).date()
    # end_date = datetime(2023, 12, 17).date()
    
    num_days = (end_date - start_date).days + 1  
    extended_dates = []
    for day in tqdm(range(num_days)):
        current_date = start_date + timedelta(days=day)
        extended_dates.append("https://core-api.nba.com/cp/api/v1.3/feeds/gamecardfeed?gamedate="+str(current_date.strftime("%m/%d/%Y")) +"&platform=web")



    def process_url(url):

        try:
            response = check_requests(url, headers)
            if response:
                datas = response.json()

                for i in datas['modules'][0]['cards']:
                    game_id = i['cardData']['gameId']
                    day = i['cardData']['gameTimeEastern']
                    # comment
                    for period in range(1, 5):
                        url = f"https://stats.nba.com/stats/boxscorehustlev2?GameID={game_id}&LeagueID=00&endPeriod={period}&endRange=28800&rangeType=1&startPeriod={period}&startRange=0"
                        response = check_requests(url, headerss)
                        if response:
                            datas = response.json()
                            periods = split_periods(url)
                            gameid = datas['boxScoreHustle']['gameId']
                            away_team_id = datas['boxScoreHustle']['awayTeamId']
                            home_team_id = datas['boxScoreHustle']['homeTeamId']
                            all_players = datas['boxScoreHustle']['awayTeam']['players'] + datas['boxScoreHustle']['homeTeam']['players']
                            team_types = ['awayTeam'] * len(datas['boxScoreHustle']['awayTeam']['players']) + \
                                        ['homeTeam'] * len(datas['boxScoreHustle']['homeTeam']['players'])

                            for player, team_type in zip(all_players, team_types):
                                data = dict()
                                time = datas['meta'].get('time')
                                data['day'] = split_day(day)
                                data['game_id'] = int(gameid)
                                data['team_name'] = datas['boxScoreHustle'][team_type]['teamCity'] + ' ' + datas['boxScoreHustle'][team_type]['teamName']
                                data['player_name'] = player['firstName'] + ' ' + player['familyName']
                                # data['periods'] = "Q" + str(periods)
                                # data['min'] = (player['statistics'].get('minutes'))
                                data['min'] = (player['statistics'].get('minutes'))
                                data['screen_ast'] = round(float(player['statistics'].get('screenAssists')),2)
                                data['screen_ast_pts'] = round(float(player['statistics'].get('screenAssistPoints')),2)
                                data['deflections'] = round(float(player['statistics'].get('deflections')),2)
                                data['off_loose_balls_recovered'] = round(float(player['statistics'].get('looseBallsRecoveredOffensive')),2)
                                data['def_loose_balls_recovered'] = round(float(player['statistics'].get('looseBallsRecoveredDefensive')),2)
                                data['loose_balls_recovered'] = round(float(player['statistics'].get('looseBallsRecoveredTotal')),2)
                                data['charges_drawn'] = round(float(player['statistics'].get('chargesDrawn')),2)
                                data['contested_2pt_shots'] = round(float(player['statistics'].get('contestedShots2pt')),2)
                                data['contested_3pt_shots'] = round(float(player['statistics'].get('contestedShots3pt')),2)
                                data['contested_shots'] = round(float(player['statistics'].get('contestedShots')),2)
                                data['off_box_outs'] = round(float(player['statistics'].get('offensiveBoxOuts')),2)
                                data['def_box_outs'] = round(float(player['statistics'].get('defensiveBoxOuts')),2)
                                data['box_outs'] = round(float(player['statistics'].get('boxOuts')),2)
                                data['position'] = player['position']
                                data['team_id'] = int(away_team_id if team_type == 'awayTeam' else home_team_id)
                                with write_lock:
                                    df = pd.DataFrame([data])
                                    df.to_csv("./data/boxscores_hustle.csv", mode='a', header=not pd.io.common.file_exists("./data/boxscores_hustle.csv"), index=False)
        except IndexError as e:
            print(f"Index error occurred: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(process_url, extended_dates)
    
    
    
    
    
def boxscores_misc():

    import requests
    from datetime import datetime, timedelta
    from concurrent.futures import ThreadPoolExecutor
    from tqdm import tqdm
    import pandas as pd
    from threading import Lock
    write_lock = Lock() 
    


    headers = {
        'authority': 'core-api.nba.com',
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'ocp-apim-subscription-key': '747fa6900c6c4e89a58b81b72f36eb96',
        'origin': 'https://www.nba.com',
        'pragma': 'no-cache',
        'referer': 'https://www.nba.com/',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
    }
    headerss = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Origin': 'https://www.nba.com',
        'Referer': 'https://www.nba.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }






    def check_requests(url, HEADERS=None):
        check_out = 3
        res = None
        while check_out > 0:
            res = requests.get(url, headers=HEADERS)
            if res.status_code == 200: 
                break
            check_out -= 1
        return res


    def split_periods(period):
        return period.split('&')[-2].replace('startPeriod=','')
    def split_day (day):
        return day.split('T')[0]

        
    # Ngày hiện tại
    current_time = datetime.now().date()- timedelta(days=1)
    previous_day = current_time - timedelta(days=2)

    # start_date = datetime(2023, 10, 25).date()


    start_date = datetime(int(str(previous_day).split("-")[0]), int(str(previous_day).split("-")[1]), int(str(previous_day).split("-")[2])).date()
    end_date = datetime(int(str(current_time).split("-")[0]), int(str(current_time).split("-")[1]), int(str(current_time).split("-")[2])).date()
    # end_date = datetime(2021, 11, 17)
    # start_date = datetime(2023, 10, 25).date()
    # end_date = datetime(2023, 12, 17).date()
    num_days = (end_date - start_date).days + 1  
    extended_dates = []
    for day in tqdm(range(num_days)):
        current_date = start_date + timedelta(days=day)
        extended_dates.append("https://core-api.nba.com/cp/api/v1.3/feeds/gamecardfeed?gamedate="+str(current_date.strftime("%m/%d/%Y")) +"&platform=web")



    def process_url(url):

        try:
            response = check_requests(url, headers)
            if response:
                datas = response.json()

                for i in datas['modules'][0]['cards']:
                    game_id = i['cardData']['gameId']
                    day = i['cardData']['gameTimeEastern']
                    # comment
                    for period in range(1, 5):
                        url = f"https://stats.nba.com/stats/boxscoremiscv3?GameID={game_id}&LeagueID=00&endPeriod={period}&endRange=28800&rangeType=1&startPeriod={period}&startRange=0"
                        response = check_requests(url, headerss)
                        if response:
                            datas = response.json()
                            periods = split_periods(url)
                            gameid = datas['boxScoreMisc']['gameId']
                            away_team_id = datas['boxScoreMisc']['awayTeamId']
                            home_team_id = datas['boxScoreMisc']['homeTeamId']
                            all_players = datas['boxScoreMisc']['awayTeam']['players'] + datas['boxScoreMisc']['homeTeam']['players']
                            team_types = ['awayTeam'] * len(datas['boxScoreMisc']['awayTeam']['players']) + \
                                        ['homeTeam'] * len(datas['boxScoreMisc']['homeTeam']['players'])

                            for player, team_type in zip(all_players, team_types):
                                data = dict()
                                time = datas['meta'].get('time')
                                data['day'] = split_day(day)
                                data['game_id'] = int(gameid)
                                data['team_name'] = datas['boxScoreMisc'][team_type]['teamCity'] + ' ' + datas['boxScoreMisc'][team_type]['teamName']
                                data['player_name'] = player['firstName'] + ' ' + player['familyName']
                                data['periods'] = "Q" + str(periods)
                                # data['min'] = (player['statistics'].get('minutes'))
                                data['min'] = (player['statistics'].get('minutes'))
                                data['pts_off_to'] = round(float(player['statistics'].get('pointsOffTurnovers')), 2)
                                data['2nd_pts'] = round(float(player['statistics'].get('pointsSecondChance')),2)
                                data['fbps'] = round(float(player['statistics'].get('pointsFastBreak')),2)
                                data['pitp'] = round(float(player['statistics'].get('pointsPaint')) ,2)
                                data['opp_pts_off_to'] = round(float(player['statistics'].get('oppPointsOffTurnovers')),2)
                                data['opp_2nd_pts'] = round(float(player['statistics'].get('oppPointsSecondChance')),2)
                                data['opp_fbps'] = round(float(player['statistics'].get('oppPointsFastBreak')),2)
                                data['opp_pitp'] = round(float(player['statistics'].get('oppPointsPaint')),2)
                                data['blk'] = round(float(player['statistics'].get('blocks')),2)
                                data['bka'] = round(float(player['statistics'].get('blocksAgainst')),2)
                                data['pf'] = round(float(player['statistics'].get('foulsPersonal')),2)
                                data['fd'] = round(float(player['statistics'].get('foulsDrawn')),2)
                                data['position'] = player['position']
                                data['team_id'] = int(away_team_id if team_type == 'awayTeam' else home_team_id)
                                with write_lock:
                                    df = pd.DataFrame([data])
                                    df.to_csv("./data/boxscores_misc.csv", mode='a', header=not pd.io.common.file_exists("./data/boxscores_misc.csv"), index=False)
        except IndexError as e:
            print(f"Index error occurred: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(process_url, extended_dates)




def boxscores_scoring():

    import requests
    from datetime import datetime, timedelta
    from concurrent.futures import ThreadPoolExecutor
    from tqdm import tqdm
    import pandas as pd
    from threading import Lock
    write_lock = Lock()


    headers = {
        'authority': 'core-api.nba.com',
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'ocp-apim-subscription-key': '747fa6900c6c4e89a58b81b72f36eb96',
        'origin': 'https://www.nba.com',
        'pragma': 'no-cache',
        'referer': 'https://www.nba.com/',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
    }
    headerss = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Origin': 'https://www.nba.com',
        'Referer': 'https://www.nba.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }






    def check_requests(url, HEADERS=None):
        check_out = 3
        res = None
        while check_out > 0:
            res = requests.get(url, headers=HEADERS)
            if res.status_code == 200: 
                break
            check_out -= 1
        return res


    def split_periods(period):
        return period.split('&')[-2].replace('startPeriod=','')
    def split_day (day):
        return day.split('T')[0]

        
    # Ngày hiện tại
    current_time = datetime.now().date()- timedelta(days=1)
    previous_day = current_time - timedelta(days=2)

    # start_date = datetime(2023, 10, 25).date()


    start_date = datetime(int(str(previous_day).split("-")[0]), int(str(previous_day).split("-")[1]), int(str(previous_day).split("-")[2])).date()
    end_date = datetime(int(str(current_time).split("-")[0]), int(str(current_time).split("-")[1]), int(str(current_time).split("-")[2])).date()
    # end_date = datetime(2021, 11, 17)
    # start_date = datetime(2023, 10, 25).date()
    # end_date = datetime(2023, 12, 17).date()
    num_days = (end_date - start_date).days + 1  
    extended_dates = []
    for day in tqdm(range(num_days)):
        current_date = start_date + timedelta(days=day)
        extended_dates.append("https://core-api.nba.com/cp/api/v1.3/feeds/gamecardfeed?gamedate="+str(current_date.strftime("%m/%d/%Y")) +"&platform=web")



    def process_url(url):

        try:
            response = check_requests(url, headers)
            # print(response.status_code)
            # print(response.status_code)
            if response.status_code == 200:
                datas = response.json() 
                # print(response.status_code)
                for i in datas['modules'][0]['cards']:
                    game_id = i['cardData']['gameId']
                    day = i['cardData']['gameTimeEastern']

                    for period in range(1, 5):
                        url = f"https://stats.nba.com/stats/boxscorescoringv3?GameID={game_id}&LeagueID=00&endPeriod={period}&endRange=28800&rangeType=1&startPeriod={period}&startRange=0"
                        response = check_requests(url, headerss)
                        if response:
                            datas = response.json()
                            periods = split_periods(url)
                            gameid = datas['boxScoreScoring']['gameId']
                            away_team_id = datas['boxScoreScoring']['awayTeamId']
                            home_team_id = datas['boxScoreScoring']['homeTeamId']
                            all_players = datas['boxScoreScoring']['awayTeam']['players'] + datas['boxScoreScoring']['homeTeam']['players']
                            team_types = ['awayTeam'] * len(datas['boxScoreScoring']['awayTeam']['players']) + \
                                        ['homeTeam'] * len(datas['boxScoreScoring']['homeTeam']['players'])

                            for player, team_type in zip(all_players, team_types):
                                data = dict()
                                time = datas['meta'].get('time')
                                data['day'] = split_day(day)
                                data['game_id'] = int(gameid)
                                data['team_name'] = datas['boxScoreScoring'][team_type]['teamCity'] + ' ' + datas['boxScoreScoring'][team_type]['teamName']
                                data['player_name'] = player['firstName'] + ' ' + player['familyName']
                                data['periods'] = "Q" + str(periods)
                                data['min'] = (player['statistics'].get('minutes'))
                                data['percent_fga_2pt'] = round(float(player['statistics'].get('percentageFieldGoalsAttempted2pt'))*100, 2)
                                data['percent_fga_3pt'] = round(float(player['statistics'].get('percentageFieldGoalsAttempted3pt'))*100,2)
                                data['percent_pts_2pt'] = round(float(player['statistics'].get('percentagePoints2pt'))*100,2)
                                data['percent_pts_2pt_mr'] = round(float(player['statistics'].get('percentagePointsMidrange2pt'))*100 ,2)
                                data['percent_pts_3pt'] = round(float(player['statistics'].get('percentagePoints3pt'))*100,2)
                                data['percent_pts_fbps'] = round(float(player['statistics'].get('percentagePointsFastBreak'))*100,2)
                                data['percent_pts_ft'] = round(float(player['statistics'].get('percentagePointsFreeThrow'))*100,2)
                                data['percent_pts_offto'] = round(float(player['statistics'].get('percentagePointsOffTurnovers'))*100,2)
                                data['percent_pts_pitp'] = round(float(player['statistics'].get('percentagePointsPaint'))*100,2)
                                data['2fgm_percent_ast'] = round(float(player['statistics'].get('percentageAssisted2pt'))*100,2)
                                data['2fgm_percent_uast'] = round(float(player['statistics'].get('percentageUnassisted2pt'))*100,2)
                                data['3fgm_percent_ast'] = round(float(player['statistics'].get('percentageAssisted3pt'))*100,2)
                                data['3fgm_percent_uast'] = round(float(player['statistics'].get('percentageUnassisted3pt'))*100,2)
                                data['fgm_percent_ast'] = round(float(player['statistics'].get('percentageAssistedFGM'))*100,2)
                                data['fgm_percent_uast'] = round(float(player['statistics'].get('percentageUnassistedFGM'))*100,2)
                                data['position'] = player['position']
                                data['team_id'] = int(away_team_id if team_type == 'awayTeam' else home_team_id)
                                with write_lock:
                                    df = pd.DataFrame([data])
                                    df.to_csv("./data/boxscores_scoring.csv", mode='a', header=not pd.io.common.file_exists("./data/boxscores_scoring.csv"), index=False)
        except IndexError as e:
            print(f"Index error occurred: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(process_url, extended_dates)




def boxscores_tracking():
    import requests
    from datetime import datetime, timedelta
    from concurrent.futures import ThreadPoolExecutor
    from tqdm import tqdm
    import pandas as pd
    from threading import Lock
    write_lock = Lock()

    headers = {
        'authority': 'core-api.nba.com',
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'ocp-apim-subscription-key': '747fa6900c6c4e89a58b81b72f36eb96',
        'origin': 'https://www.nba.com',
        'pragma': 'no-cache',
        'referer': 'https://www.nba.com/',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
    }
    headerss = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Origin': 'https://www.nba.com',
        'Referer': 'https://www.nba.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }






    def check_requests(url, HEADERS=None):
        check_out = 3
        res = None
        while check_out > 0:
            res = requests.get(url, headers=HEADERS)
            if res.status_code == 200: 
                break
            check_out -= 1
        return res


    def split_periods(period):
        return period.split('&')[-2].replace('startPeriod=','')
    def split_day (day):
        return day.split('T')[0]

        
    # Ngày hiện tại
    current_time = datetime.now().date()- timedelta(days=1)
    previous_day = current_time - timedelta(days=2)

    # start_date = datetime(2023, 10, 25).date()


    start_date = datetime(int(str(previous_day).split("-")[0]), int(str(previous_day).split("-")[1]), int(str(previous_day).split("-")[2])).date()
    end_date = datetime(int(str(current_time).split("-")[0]), int(str(current_time).split("-")[1]), int(str(current_time).split("-")[2])).date()
    # end_date = datetime(2021, 11, 17)
    # start_date = datetime(2023, 10, 25).date()
    # end_date = datetime(2023, 12, 17).date()
    num_days = (end_date - start_date).days + 1  
    extended_dates = []
    for day in tqdm(range(num_days)):
        current_date = start_date + timedelta(days=day)
        extended_dates.append("https://core-api.nba.com/cp/api/v1.3/feeds/gamecardfeed?gamedate="+str(current_date.strftime("%m/%d/%Y")) +"&platform=web")



    def process_url(url):

        try:
            response = check_requests(url, headers)
            if response:
                datas = response.json()

                for i in datas['modules'][0]['cards']:
                    game_id = i['cardData']['gameId']
                    day = i['cardData']['gameTimeEastern']
                    # comment
                    for period in range(1, 2):
                        url = f"https://stats.nba.com/stats/boxscoreplayertrackv3?GameID={game_id}&LeagueID=00&endPeriod={period}&endRange=28800&rangeType=1&startPeriod={period}&startRange=0"
                        response = check_requests(url, headerss)
                        if response:
                            datas = response.json()
                            periods = split_periods(url)
                            gameid = datas['boxScorePlayerTrack']['gameId']
                            away_team_id = datas['boxScorePlayerTrack']['awayTeamId']
                            home_team_id = datas['boxScorePlayerTrack']['homeTeamId']
                            all_players = datas['boxScorePlayerTrack']['awayTeam']['players'] + datas['boxScorePlayerTrack']['homeTeam']['players']
                            team_types = ['awayTeam'] * len(datas['boxScorePlayerTrack']['awayTeam']['players']) + \
                                        ['homeTeam'] * len(datas['boxScorePlayerTrack']['homeTeam']['players'])

                            for player, team_type in zip(all_players, team_types):
                                data = dict()
                                time = datas['meta'].get('time')
                                data['day'] = split_day(day)
                                data['game_id'] = int(gameid)
                                data['team_name'] = datas['boxScorePlayerTrack'][team_type]['teamCity'] + ' ' + datas['boxScorePlayerTrack'][team_type]['teamName']
                                data['player_name'] = player['firstName'] + ' ' + player['familyName']
                                data['periods'] = "Q" + str(periods)
                                # data['min'] = (player['statistics'].get('minutes'))
                                data['min'] = (player['statistics'].get('minutes'))
                                data['spd'] = round(float(player['statistics'].get('speed')), 2)
                                data['dist'] = round(float(player['statistics'].get('distance')),2)
                                data['orbc'] = round(float(player['statistics'].get('reboundChancesOffensive')),2)
                                data['drbc'] = round(float(player['statistics'].get('reboundChancesDefensive')),2)
                                data['rbc'] = round(float(player['statistics'].get('reboundChancesTotal')),2)
                                data['tchs'] = round(float(player['statistics'].get('touches')),2)
                                data['sast'] = round(float(player['statistics'].get('secondaryAssists')),2)
                                data['ft_ast'] = round(float(player['statistics'].get('freeThrowAssists')),2)
                                data['pass'] = round(float(player['statistics'].get('passes')),2)
                                data['ast'] = round(float(player['statistics'].get('assists')),2)
                                data['cfgm'] = round(float(player['statistics'].get('contestedFieldGoalsMade')),2)
                                data['cfga'] = round(float(player['statistics'].get('contestedFieldGoalsAttempted')),2)
                                data['cfg_percent'] = round(float(player['statistics'].get('contestedFieldGoalPercentage'))*100,2)
                                data['ufgm'] = round(float(player['statistics'].get('uncontestedFieldGoalsMade')),2)
                                data['ufga'] = round(float(player['statistics'].get('uncontestedFieldGoalsAttempted')),2)
                                data['ufg_percent'] = round(float(player['statistics'].get('uncontestedFieldGoalsPercentage'))*100,2)
                                data['fg_percent'] = round(float(player['statistics'].get('fieldGoalPercentage'))*100,2)
                                data['dfgm'] = round(float(player['statistics'].get('defendedAtRimFieldGoalsMade')),2)
                                data['dfga'] = round(float(player['statistics'].get('defendedAtRimFieldGoalsAttempted')),2)
                                data['dfg_percent'] = round(float(player['statistics'].get('defendedAtRimFieldGoalPercentage'))*100,2) 
                                data['position'] = player['position']
                                data['team_id'] = int(away_team_id if team_type == 'awayTeam' else home_team_id)
                                with write_lock:
                                    df = pd.DataFrame([data])
                                    df.to_csv("./data/boxscores_tracking.csv", mode='a', header=not pd.io.common.file_exists("./data/boxscores_tracking.csv"), index=False)
        except IndexError as e:
            print(f"Index error occurred: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(process_url, extended_dates)
    
    
    
def boxscores_traditional():



    import requests
    from datetime import datetime, timedelta
    from concurrent.futures import ThreadPoolExecutor
    from tqdm import tqdm
    import pandas as pd
    from threading import Lock
    write_lock = Lock()


    headers = {
        'authority': 'core-api.nba.com',
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'ocp-apim-subscription-key': '747fa6900c6c4e89a58b81b72f36eb96',
        'origin': 'https://www.nba.com',
        'pragma': 'no-cache',
        'referer': 'https://www.nba.com/',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
    }
    headerss = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Origin': 'https://www.nba.com',
        'Referer': 'https://www.nba.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }






    def check_requests(url, HEADERS=None):
        check_out = 3
        res = None
        while check_out > 0:
            res = requests.get(url, headers=HEADERS)
            if res.status_code == 200: 
                break
            check_out -= 1
        return res


    def split_periods(period):
        return period.split('&')[-2].replace('startPeriod=','')
    def split_day (day):
        return day.split('T')[0]

        
    # Ngày hiện tại
    current_time = datetime.now().date()- timedelta(days=1)
    previous_day = current_time - timedelta(days=2)

    # start_date = datetime(2023, 10, 25).date()


    start_date = datetime(int(str(previous_day).split("-")[0]), int(str(previous_day).split("-")[1]), int(str(previous_day).split("-")[2])).date()
    end_date = datetime(int(str(current_time).split("-")[0]), int(str(current_time).split("-")[1]), int(str(current_time).split("-")[2])).date()
    # end_date = datetime(2021, 11, 17)
    # start_date = datetime(2023, 10, 25).date()
    # end_date = datetime(2023, 12, 17).date()
    num_days = (end_date - start_date).days + 1  
    extended_dates = []
    for day in tqdm(range(num_days)):
        current_date = start_date + timedelta(days=day)
        extended_dates.append("https://core-api.nba.com/cp/api/v1.3/feeds/gamecardfeed?gamedate="+str(current_date.strftime("%m/%d/%Y")) +"&platform=web")



    def process_url(url):

        try:
            response = check_requests(url, headers)
            if response:
                datas = response.json()

                for i in datas['modules'][0]['cards']:
                    game_id = i['cardData']['gameId']
                    day = i['cardData']['gameTimeEastern']
                    # comment
                    for period in range(1, 5):
                        url = f"https://stats.nba.com/stats/boxscoretraditionalv3?GameID={game_id}&LeagueID=00&endPeriod={period}&endRange=28800&rangeType=1&startPeriod={period}&startRange=0"
                        response = check_requests(url, headerss)
                        if response:
                            datas = response.json()
                            periods = split_periods(url)
                            gameid = datas['boxScoreTraditional']['gameId']
                            away_team_id = datas['boxScoreTraditional']['awayTeamId']
                            home_team_id = datas['boxScoreTraditional']['homeTeamId']
                            all_players = datas['boxScoreTraditional']['awayTeam']['players'] + datas['boxScoreTraditional']['homeTeam']['players']
                            team_types = ['awayTeam'] * len(datas['boxScoreTraditional']['awayTeam']['players']) + \
                                        ['homeTeam'] * len(datas['boxScoreTraditional']['homeTeam']['players'])

                            for player, team_type in zip(all_players, team_types):
                                data = dict()
                                time = datas['meta'].get('time')
                                data['day'] = split_day(day)
                                data['game_id'] = int(gameid)
                                data['team_name'] = datas['boxScoreTraditional'][team_type]['teamCity'] + ' ' + datas['boxScoreTraditional'][team_type]['teamName']
                                data['player_name'] = player['firstName'] + ' ' + player['familyName']
                                data['periods'] = "Q" + str(periods)
                                # data['min'] = (player['statistics'].get('minutes'))
                                data['min'] = (player['statistics'].get('minutes'))
                                data['fgm'] = round(float(player['statistics'].get('fieldGoalsMade')), 2)
                                data['fga'] = round(float(player['statistics'].get('fieldGoalsAttempted')),2)
                                data['fg_percent'] = round(float(player['statistics'].get('fieldGoalsPercentage'))*100,2)
                                data['3pm'] = round(float(player['statistics'].get('threePointersMade')) ,2)
                                data['3pa'] = round(float(player['statistics'].get('threePointersAttempted')),2)
                                data['3p_percent'] = round(float(player['statistics'].get('threePointersPercentage'))*100,2)
                                data['ftm'] = round(float(player['statistics'].get('freeThrowsMade')),2)
                                data['fta'] = round(float(player['statistics'].get('freeThrowsAttempted')),2)
                                data['ft_percent'] = round(float(player['statistics'].get('freeThrowsPercentage'))*100,2)
                                data['oreb'] = round(float(player['statistics'].get('reboundsOffensive')),2)
                                data['dreb'] = round(float(player['statistics'].get('reboundsDefensive')),2)
                                data['reb'] = round(float(player['statistics'].get('reboundsTotal')),2)
                                data['ast'] = round(float(player['statistics'].get('assists')),2)
                                data['stl'] = round(float(player['statistics'].get('steals')),2)
                                data['blk'] = round(float(player['statistics'].get('blocks')),2)
                                data['to'] = round(float(player['statistics'].get('turnovers')),2)
                                data['pf'] = round(float(player['statistics'].get('foulsPersonal')),2)
                                data['pts'] = round(float(player['statistics'].get('points')),2)
                                data['plus_minus'] = round(float(player['statistics'].get('plusMinusPoints')),2)
                                data['position'] = player['position']
                                data['team_id'] = int(away_team_id if team_type == 'awayTeam' else home_team_id)
                                with write_lock:
                                    df = pd.DataFrame([data])
                                    df.to_csv("./data/boxscores_traditional.csv", mode='a', header=not pd.io.common.file_exists("./data/boxscores_traditional.csv"), index=False)
        except IndexError as e:
            print(f"Index error occurred: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(process_url, extended_dates)




def boxscores_usage():



    import requests
    from datetime import datetime, timedelta
    from concurrent.futures import ThreadPoolExecutor
    from tqdm import tqdm
    import pandas as pd
    from threading import Lock
    write_lock = Lock()



    headers = {
        'authority': 'core-api.nba.com',
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'ocp-apim-subscription-key': '747fa6900c6c4e89a58b81b72f36eb96',
        'origin': 'https://www.nba.com',
        'pragma': 'no-cache',
        'referer': 'https://www.nba.com/',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
    }
    headerss = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Origin': 'https://www.nba.com',
        'Referer': 'https://www.nba.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }






    def check_requests(url, HEADERS=None):
        check_out = 3
        res = None
        while check_out > 0:
            res = requests.get(url, headers=HEADERS)
            if res.status_code == 200: 
                break
            check_out -= 1
        return res


    def split_periods(period):
        return period.split('&')[-2].replace('startPeriod=','')
    def split_day (day):
        return day.split('T')[0]

        
    # Ngày hiện tại
    current_time = datetime.now().date()- timedelta(days=1)
    previous_day = current_time - timedelta(days=2)

    # start_date = datetime(2023, 10, 25).date()


    start_date = datetime(int(str(previous_day).split("-")[0]), int(str(previous_day).split("-")[1]), int(str(previous_day).split("-")[2])).date()
    end_date = datetime(int(str(current_time).split("-")[0]), int(str(current_time).split("-")[1]), int(str(current_time).split("-")[2])).date()
    # end_date = datetime(2021, 11, 17)
    # start_date = datetime(2023, 10, 25).date()
    # end_date = datetime(2023, 12, 17).date()
    num_days = (end_date - start_date).days + 1  
    extended_dates = []
    for day in tqdm(range(num_days)):
        current_date = start_date + timedelta(days=day)
        extended_dates.append("https://core-api.nba.com/cp/api/v1.3/feeds/gamecardfeed?gamedate="+str(current_date.strftime("%m/%d/%Y")) +"&platform=web")



    def process_url(url):

        try:
            response = check_requests(url, headers)
            if response:
                datas = response.json()

                for i in datas['modules'][0]['cards']:
                    game_id = i['cardData']['gameId']
                    day = i['cardData']['gameTimeEastern']
                    # comment
                    for period in range(1, 5):
                        url = f"https://stats.nba.com/stats/boxscoreusagev3?GameID={game_id}&LeagueID=00&endPeriod={period}&endRange=28800&rangeType=1&startPeriod={period}&startRange=0"
                        response = check_requests(url, headerss)
                        if response:
                            datas = response.json()
                            periods = split_periods(url)
                            gameid = datas['boxScoreUsage']['gameId']
                            away_team_id = datas['boxScoreUsage']['awayTeamId']
                            home_team_id = datas['boxScoreUsage']['homeTeamId']
                            all_players = datas['boxScoreUsage']['awayTeam']['players'] + datas['boxScoreUsage']['homeTeam']['players']
                            team_types = ['awayTeam'] * len(datas['boxScoreUsage']['awayTeam']['players']) + \
                                        ['homeTeam'] * len(datas['boxScoreUsage']['homeTeam']['players'])

                            for player, team_type in zip(all_players, team_types):
                                data = dict()
                                time = datas['meta'].get('time')
                                data['day'] = split_day(day)
                                data['game_id'] = int(gameid)
                                data['team_name'] = datas['boxScoreUsage'][team_type]['teamCity'] + ' ' + datas['boxScoreUsage'][team_type]['teamName']
                                data['player_name'] = player['firstName'] + ' ' + player['familyName']
                                data['periods'] = "Q" + str(periods)
                                data['min'] = (player['statistics'].get('minutes'))
                                data['usg_percent'] = round(float(player['statistics'].get('usagePercentage'))*100, 2)
                                data['percent_fgm'] = round(float(player['statistics'].get('percentageFieldGoalsMade'))*100,2)
                                data['percent_fga'] = round(float(player['statistics'].get('percentageFieldGoalsAttempted'))*100,2)
                                data['percent_3pm'] = round(float(player['statistics'].get('percentageThreePointersMade'))*100,2)
                                data['percent_3pa'] = round(float(player['statistics'].get('percentageThreePointersAttempted'))*100,2)
                                data['percent_ftm'] = round(float(player['statistics'].get('percentageFreeThrowsMade'))*100,2)
                                data['percent_fta'] = round(float(player['statistics'].get('percentageFreeThrowsAttempted'))*100,2)
                                data['percent_oreb'] = round(float(player['statistics'].get('percentageReboundsOffensive'))*100,2)
                                data['percent_dreb'] = round(float(player['statistics'].get('percentageReboundsDefensive'))*100,2)
                                data['percent_reb'] = round(float(player['statistics'].get('percentageReboundsTotal'))*100,2)
                                data['percent_ast'] = round(float(player['statistics'].get('percentageAssists'))*100,2)
                                data['percent_to'] = round(float(player['statistics'].get('percentageTurnovers'))*100,2)
                                data['percent_stl'] = round(float(player['statistics'].get('percentageSteals'))*100,2)
                                data['percent_blk'] = round(float(player['statistics'].get('percentageBlocks'))*100,2)
                                data['percent_blka'] = round(float(player['statistics'].get('percentageBlocksAllowed'))*100,2)
                                data['percent_pf'] = round(float(player['statistics'].get('percentagePersonalFouls'))*100,2)
                                data['percent_pfd'] = round(float(player['statistics'].get('percentagePersonalFoulsDrawn'))*100,2)
                                data['percent_pts'] = round(float(player['statistics'].get('percentagePoints'))*100,2)
                                data['position'] = player['position']
                                data['team_id'] = int(away_team_id if team_type == 'awayTeam' else home_team_id)
                                with write_lock:
                                    df = pd.DataFrame([data])
                                    df.to_csv("./data/boxscores_usage.csv", mode='a', header=not pd.io.common.file_exists("./data/boxscores_usage.csv"), index=False)
        except IndexError as e:
            print(f"Index error occurred: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(process_url, extended_dates)



def boxscores():
        
        
    import requests
    from datetime import datetime, timedelta
    from concurrent.futures import ThreadPoolExecutor
    from tqdm import tqdm
    import pandas as pd


    headersss = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Origin': 'https://www.nba.com',
        'Pragma': 'no-cache',
        'Referer': 'https://www.nba.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    paramsss = {
        'Conference': '',
        'DateFrom': '',
        'DateTo': '',
        'Division': '',
        'GameScope': '',
        'GameSegment': '',
        'Height': '',
        'ISTRound': '',
        'LastNGames': '0',
        'LeagueID': '00',
        'Location': '',
        'MeasureType': 'Base',
        'Month': '0',
        'OpponentTeamID': '0',
        'Outcome': '',
        'PORound': '0',
        'PaceAdjust': 'N',
        'PerMode': 'PerGame',
        'Period': '0',
        'PlayerExperience': '',
        'PlayerPosition': '',
        'PlusMinus': 'N',
        'Rank': 'N',
        'Season': '2023-24',
        'SeasonSegment': '',
        'SeasonType': 'Regular Season',
        'ShotClockRange': '',
        'StarterBench': '',
        'TeamID': '0',
        'TwoWay': '0',
        'VsConference': '',
        'VsDivision': '',
    }

    response = requests.get('https://stats.nba.com/stats/leaguedashteamstats', params=paramsss, headers=headersss)
    datas = response.json() 
    rows = datas['resultSets'][0]['rowSet']
    list_data = []
    for i in tqdm(rows):
        data = dict()
        data["team_id"] = i[0]
        data["team_name"] = i[1]
        list_data.append(data)

    df = pd.DataFrame(list_data)
    df.to_csv("./data/boxscores.csv",index= False)
    
    


def game_history():
    
    import requests
    import concurrent.futures
    import time
    import json
    from datetime import datetime, timedelta
    import pandas as pd
    from tqdm import tqdm



    headers = {
        'authority': 'core-api.nba.com',
        'accept': 'application/json',
        'accept-language': 'en,vi;q=0.9',
        'cache-control': 'no-cache',
        'ocp-apim-subscription-key': '747fa6900c6c4e89a58b81b72f36eb96',
        'origin': 'https://www.nba.com',
        'pragma': 'no-cache',
        'referer': 'https://www.nba.com/',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
    }
    # url = 'https://core-api.nba.com/cp/api/v1.3/feeds/gamecardfeed?gamedate=04/26/2023&platform=web'
    teams_dict = {
        1610612737: 'Atlanta Hawks',
        1610612738: 'Boston Celtics',
        1610612751: 'Brooklyn Nets',
        1610612766: 'Charlotte Hornets',
        1610612741: 'Chicago Bulls',
        1610612739: 'Cleveland Cavaliers',
        1610612742: 'Dallas Mavericks',
        1610612743: 'Denver Nuggets',
        1610612765: 'Detroit Pistons',
        1610612744: 'Golden State Warriors',
        1610612745: 'Houston Rockets',
        1610612754: 'Indiana Pacers',
        1610612746: 'LA Clippers',
        1610612747: 'Los Angeles Lakers',
        1610612763: 'Memphis Grizzlies',
        1610612748: 'Miami Heat',
        1610612749: 'Milwaukee Bucks',
        1610612750: 'Minnesota Timberwolves',
        1610612740: 'New Orleans Pelicans',
        1610612752: 'New York Knicks',
        1610612760: 'Oklahoma City Thunder',
        1610612753: 'Orlando Magic',
        1610612755: 'Philadelphia 76ers',
        1610612756: 'Phoenix Suns',
        1610612757: 'Portland Trail Blazers',
        1610612758: 'Sacramento Kings',
        1610612759: 'San Antonio Spurs',
        1610612761: 'Toronto Raptors',
        1610612762: 'Utah Jazz',
        1610612764: 'Washington Wizards'
    }
# Ngày hiện tại
    current_time = datetime.now().date()- timedelta(days=1)
    previous_day = current_time - timedelta(days=2)

    # start_date = datetime(2023, 10, 25).date()


    start_date = datetime(int(str(previous_day).split("-")[0]), int(str(previous_day).split("-")[1]), int(str(previous_day).split("-")[2])).date()
    end_date = datetime(int(str(current_time).split("-")[0]), int(str(current_time).split("-")[1]), int(str(current_time).split("-")[2])).date()
    
    num_days = (end_date - start_date).days + 1  
    extended_dates = []
    for day in tqdm(range(num_days)):
        current_date = start_date + timedelta(days=day)
        extended_dates.append("https://core-api.nba.com/cp/api/v1.3/feeds/gamecardfeed?gamedate="+str(current_date.strftime("%m/%d/%Y")) +"&platform=web")

    list_data = []
    def get_his(url,headers):
        response = requests.get(url, headers=headers)
        taget = response.json()
        count = 1
        for i in taget["modules"][0]["cards"]:
            data =dict()
            
            data["game_id"]= int(i["cardData"].get("gameId"))
            data["year"]= (i["cardData"].get("gameTimeEastern").split("T")[0].split("-")[0])
            data["date"]= str(i["cardData"].get("gameTimeEastern").split("T")[0].split("-")[2])+ "/" + str(i["cardData"].get("gameTimeEastern").split("T")[0].split("-")[1])
            data["team_1"]= teams_dict.get(i["cardData"]["awayTeam"].get("teamId"),i["cardData"]["awayTeam"].get("teamId"))
            data["team_2"]= teams_dict.get(i["cardData"]["homeTeam"].get("teamId"),i["cardData"]["homeTeam"].get("teamId"))
            data["period"]= (i["cardData"].get("period"))   
            data["game"]= (count)
            data["t1_score"]= (i["cardData"]["awayTeam"].get("score"))
            data["t2_score"]= (i["cardData"]["homeTeam"].get("score"))
            data["seri_score"]= (str(i["cardData"]["awayTeam"].get("wins")) + "-" + str(i["cardData"]["awayTeam"].get("losses")))
            p1 = i["cardData"]["awayTeam"]['teamLeader'].get("points")
            r1 = i["cardData"]["awayTeam"]['teamLeader'].get("rebounds")
            a1 = i["cardData"]["awayTeam"]['teamLeader'].get("assists")
            p2 = i["cardData"]["homeTeam"]['teamLeader'].get("points")
            r2 = i["cardData"]["homeTeam"]['teamLeader'].get("rebounds")
            a2 = i["cardData"]["homeTeam"]['teamLeader'].get("assists")
            data["t1_gamelead"]= (i["cardData"]["awayTeam"]['teamLeader'].get("name") + f"({p1}, {r1}, {a1})")
            data["t2_gamelead"]= (i["cardData"]["homeTeam"]['teamLeader'].get("name") + f"({p2}, {r2}, {a2})")
            data["team_1_id"]= (i["cardData"]["awayTeam"].get("teamId"))
            data["team_2_id"]= (i["cardData"]["homeTeam"].get("teamId"))
            count += 1
            list_data.append(data)

    with concurrent.futures.ThreadPoolExecutor(50) as executor:
        executor.map(get_his, extended_dates, [headers]*len(extended_dates))
        
    df = pd.DataFrame(list_data)
    df.to_csv("./data/game_history.csv", index=False)








