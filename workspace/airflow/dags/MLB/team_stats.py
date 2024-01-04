#___________________________________________________________________________Pitching___________________________________________________________________________
def team_stats():
    import requests
    import pandas as pd
    import numpy as np
    import concurrent.futures
    import json
    import os
    import tqdm
    from MLB.config.params import START_YEAR, END_YEAR
    from threading import Lock
    write_lock = Lock()

    list_url = [f"https://bdfed.stitch.mlbinfra.com/bdfed/stats/team?stitch_env=prod&sportId=1&gameType={t}&group=hitting&order=desc&sortStat=onBasePlusSlugging&stats=season&season={i}&limit=1000&offset=0" for i in range(START_YEAR, END_YEAR + 1) for t in ['R', 'P', 'F', 'D', 'L', 'W', 'A', 'S']]


    def get_player_pitching(url):
        response = requests.get(url)
        taget = response.json()

        session = url.split('&')[2].split('=')[1]
        session_mapping = {'R': 'regular_season', 'P': 'postseason', 'F': 'wild_card', 'D': 'division_series', 'L': 'league_championship_series', 'W': 'world_series', 'A': 'all_star_game', 'S': 'spring_training'}
        session = session_mapping.get(str(session), 'spring_training')


        for i in taget['stats']:
            data_s = dict()
            data_s["team_id"] = i['teamId'] 
            data_s["type_game"] = session
            data_s["league"] = i['leagueAbbrev']
            data_s["games_played"] = i['gamesPlayed']
            data_s["year_game"] = i['year']
            data_s["at_bats"] = i['atBats']
            data_s["runs"] = i['runs']
            data_s["hits"] = i['hits']
            data_s["doubles"] = i['doubles']
            data_s["triples"] = i['triples']
            data_s["home_runs"] = i['homeRuns']
            data_s["rbi"] = i['rbi']
            data_s["base_on_balls"] = i['baseOnBalls']
            data_s["strike_outs"] = i['strikeOuts']
            data_s["stolen_bases"] = i['stolenBases']
            data_s["caught_stealing"] = i['caughtStealing']
            data_s["avg"] = i['avg']
            data_s["obp"] = i['obp']
            data_s["slg"] = i['slg']
            data_s["ops"] = i['ops']
            data_s["air_outs"] = i['airOuts']
            data_s["catchers_interference"] = i['catchersInterference']
            data_s["left_on_base"] = i['leftOnBase']
            data_s["number_of_pitches"] = i['numberOfPitches']
            data_s["stolen_base_percentage"] = i['stolenBasePercentage'].replace(".---", "")
            data_s["plate_appearances"] = i['plateAppearances']
            data_s["hit_by_pitch"] = i['hitByPitch']
            data_s["sac_bunts"] = i['sacBunts']
            data_s["sac_flies"] = i['sacFlies']
            data_s["ground_into_double_play"] = i['groundIntoDoublePlay']
            data_s["ground_outs"] = i['groundOuts']
            data_s["ground_outs_to_airouts"] = i['groundOutsToAirouts']
            data_s["total_bases"] = i['totalBases']
            data_s["intentional_walks"] = i['intentionalWalks']
            data_s["babip"] = i['babip']
            data_s["at_bats_per_home_run"] = i['atBatsPerHomeRun'].replace("-.--", "")
        
            
            
            
            with write_lock:
                df = pd.DataFrame([data_s])
                df.to_csv('./data/team_stats_hitting.csv', mode='a', header=not pd.io.common.file_exists('./data/team_stats_hitting.csv'), index=False)
            
            
            
            
    num_thread = 100
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_thread) as executor:
        executor.map(get_player_pitching, list_url)
        


    #________________________________________________________________________________________________Hitting________________________________________________________________________________________________

    import requests
    import pandas as pd
    import numpy as np
    import concurrent.futures
    import json
    import os
    import tqdm
    from MLB.config.params import START_YEAR, END_YEAR
    from threading import Lock
    write_lock = Lock()

    list_url = [f"https://bdfed.stitch.mlbinfra.com/bdfed/stats/team?stitch_env=prod&sportId=1&gameType={t}&group=pitching&order=asc&sortStat=earnedRunAverage&stats=season&season={i}&limit=1000&offset=0" for i in range(START_YEAR, END_YEAR + 1) for t in ['R', 'P', 'F', 'D', 'L', 'W', 'A', 'S']]

    def get_player_pitching(url):
        response = requests.get(url)
        taget = response.json()

        session = url.split('&')[2].split('=')[1]
        session_mapping = {'R': 'regular_season', 'P': 'postseason', 'F': 'wild_card', 'D': 'division_series', 'L': 'league_championship_series', 'W': 'world_series', 'A': 'all_star_game', 'S': 'spring_training'}
        session = session_mapping.get(str(session), 'spring_training')


        for i in taget['stats']:
            data_s = dict()
            data_s["team_id"] = i['teamId'] 
            data_s["year_game"] = i['year']
            data_s["type_game"] = session
            data_s["league"] = i['leagueAbbrev']
            data_s["wins"] = i['wins']
            data_s["losses"] = i['losses']
            data_s["games_played"] = i['gamesPlayed']
            data_s["games_started"] = i['gamesStarted']
            data_s["complete_games"] = i['completeGames']
            data_s["shutouts"] = i['shutouts']
            data_s["saves"] = i['saves']
            data_s["save_opportunities"] = i['saveOpportunities']
            data_s["innings_pitched"] = i['inningsPitched']
            data_s["hits"] = i['hits']
            data_s["runs"] = i['runs']
            data_s["earned_runs"] = i['earnedRuns']
            data_s["home_runs"] = i['homeRuns']
            data_s["hit_batsmen"] = i['hitBatsmen']
            data_s["base_on_balls"] = i['baseOnBalls']
            data_s["strike_outs"] = i['strikeOuts']
            data_s["whip"] = i['whip']
            data_s["avg"] = i['avg']
            data_s["at_bats"] = i['atBats']
            data_s["blown_saves"] = i['blownSaves']
            data_s["caught_stealing"] = i['caughtStealing']
            data_s["catchers_interference"] = i['catchersInterference']
            data_s["doubles"] = i['doubles']
            data_s["era"] = i['era']
            data_s["games_pitched"] = i['gamesPitched']
            data_s["hit_by_pitch"] = i['hitByPitch']
            data_s["hits_per9inn"] = i['hitsPer9Inn']
            data_s["home_runs_per9"] = i['homeRunsPer9']
            data_s["obp"] = i['obp']
            data_s["ops"] = i['ops']
            data_s["outs"] = i['outs']
            data_s["pickoffs"] = i['pickoffs']
            data_s["runs_scored_per9"] = i['runsScoredPer9']
            data_s["stolen_bases"] = i['stolenBases']
            data_s["stolen_base_percentage"] = i['stolenBasePercentage'].replace(".---", "")
            data_s["strikes"] = i['strikes']
            data_s["strike_percentage"] = i['strikePercentage']
            data_s["sac_bunts"] = i['sacBunts']
            data_s["slg"] = i['slg']
            data_s["sac_flies"] = i['sacFlies']
            data_s["triples"] = i['triples']
            data_s["total_bases"] = i['totalBases']
            data_s["win_percentage"] = i['winPercentage']
            data_s["batters_faced"] = i['battersFaced']
            data_s["number_of_pitches"] = i['numberOfPitches']
            data_s["pitches_per_inning"] = i['pitchesPerInning']
            data_s["games_finished"] = i['gamesFinished']
            data_s["holds"] = i['holds']
            data_s["intentional_walks"] = i['intentionalWalks']
            data_s["wild_pitches"] = i['wildPitches']
            data_s["balks"] = i['balks']
            data_s["ground_into_double_play"] = i['groundIntoDoublePlay']
            data_s["ground_outs"] = i['groundOuts']
            data_s["air_outs"] = i['airOuts']
            data_s["ground_outs_to_airouts"] = i['groundOutsToAirouts']
            data_s["strikeouts_per9inn"] = i['strikeoutsPer9Inn']
            data_s["walks_per9inn"] = i['walksPer9Inn']
            data_s["strikeout_walk_ratio"] = i['strikeoutWalkRatio'].replace("-.--", "")
            # data_s["strikessmall"] = i['strikesSmall']
            
            
        
            
            
            
            with write_lock:
                df = pd.DataFrame([data_s])
                df.to_csv('./data/team_stats_pitching.csv', mode='a', header=not pd.io.common.file_exists('./data/team_stats_pitching.csv'), index=False)
            
            
            
            
    num_thread = 100
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_thread) as executor:
        executor.map(get_player_pitching, list_url)
        
    #________________________________________________________________________________________________end________________________________________________________________________________________________