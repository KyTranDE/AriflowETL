def player_stat():
    #___________________________________________________________________________Pitching___________________________________________________________________________
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
    try:
        os.remove('./data/player_stats_pitching.csv') 
        os.remove('./data/player_stats_hitting.csv')
    except:
        pass
    # Lấy list url
    list_url = []
    list_url = [f"https://bdfed.stitch.mlbinfra.com/bdfed/stats/player?stitch_env=prod&season={i}&sportId=1&stats=season&group=pitching&gameType={t}&limit=1000&offset=0&sortStat=onBasePlusSlugging&order=desc" for i in range(START_YEAR, END_YEAR + 1) for t in ['R', 'P', 'F', 'D', 'L', 'W', 'A', 'S']] 



    def get_player_pitching(url):
        response = requests.get(url)
        taget = response.json()

        year = url.split('&')[1].split('=')[1]
        group = url.split('&')[4].split('=')[1]
        session = url.split('&')[5].split('=')[1]
        session_mapping = {'R': 'regular_season', 'P': 'postseason', 'F': 'wild_card', 'D': 'division_series', 'L': 'league_championship_series', 'W': 'world_series', 'A': 'all_star_game', 'S': 'spring_training'}
        for i in taget['stats']:
            data_s = dict()

            data_s['year_game'] = i['year']
            data_s['type_game'] = session_mapping.get(session, session)
            data_s['player_id'] = i['playerId']
            data_s['position'] = i['positionAbbrev']
            data_s['team_id'] = i['teamId']
            data_s['wins'] = i['wins']
            data_s['losses'] = i['losses']
            data_s['era'] = i['era'].replace("-.--", "").replace(".---", "")
            data_s['games_pitched'] = i['gamesPitched']
            data_s['games_started'] = i['gamesStarted']
            data_s['complete_games'] = i['completeGames']
            data_s['shutouts'] = i['shutouts']
            data_s['saves'] = i['saves']
            data_s['save_opportunities'] = i['saveOpportunities']
            data_s['innings_pitched'] = i['inningsPitched']
            data_s['hits'] = i['hits']
            data_s['runs'] = i['runs']
            data_s['earned_runs'] = i['earnedRuns']
            data_s['home_runs'] = i['homeRuns']
            data_s['hit_batsmen'] = i['hitBatsmen']
            data_s['base_on_balls'] = i['baseOnBalls']
            data_s['strike_outs'] = i['strikeOuts']
            data_s['whip'] = i['whip'].replace("-.--", "").replace(".---", "")
            data_s['avg'] = i['avg']
            data_s['air_outs'] = i['airOuts']
            data_s['at_bats'] = i['atBats']
            data_s['balls_in_play'] = i['ballsInPlay']
            data_s['base_on_balls_per9'] = i['baseOnBallsPer9'].replace("-.--", "").replace(".---", "")
            data_s['bequeathed_runners'] = i['bequeathedRunners']
            data_s['bequeathed_runners_scored'] = i['bequeathedRunnersScored']
            data_s['blown_saves'] = i['blownSaves']
            data_s['catchers_interference'] = i['catchersInterference']
            data_s['doubles'] = i['doubles']
            data_s['fly_hits'] = i['flyHits']
            data_s['fly_outs'] = i['flyOuts']
            data_s['games_played'] = i['gamesPlayed']
            data_s['gidp'] = i['gidp']
            data_s['gidp_opp'] = i['gidpOpp']
            data_s['ground_hits'] = i['groundHits']
            data_s['ground_outs'] = i['groundOuts']
            data_s['hit_by_pitch'] = i['hitByPitch']
            data_s['hits_per9'] = i['hitsPer9'].replace("-.--", "").replace(".---", "")
            data_s['hits_per9inn'] = i['hitsPer9Inn'].replace("-.--", "").replace(".---", "")
            data_s['home_runs_per9'] = i['homeRunsPer9'].replace("-.--", "").replace(".---", "")
            data_s['home_runs_per_plate_appearance'] = i['homeRunsPerPlateAppearance']
            data_s['inherited_runners'] = i['inheritedRunners']
            data_s['inherited_runners_scored'] = i['inheritedRunnersScored']
            data_s['iso'] = i['iso']
            data_s['line_hits'] = i['lineHits']
            data_s['line_outs'] = i['lineOuts']
            data_s['obp'] = i['obp']
            data_s['ops'] = i['ops']
            data_s['outs'] = i['outs']
            data_s['pitches_per_plate_appearance'] = i['pitchesPerPlateAppearance']
            data_s['pop_hits'] = i['popHits']
            data_s['pop_outs'] = i['popOuts']
            data_s['run_support'] = i['runSupport']
            data_s['runs_scored_per9'] = i['runsScoredPer9'].replace("-.--", "").replace(".---", "")
            data_s['sac_bunts'] = i['sacBunts']
            data_s['sac_flies'] = i['sacFlies']
            data_s['slg'] = i['slg']
            data_s['stolen_base_percentage'] = i['stolenBasePercentage'].replace(".---", "").replace("-.--", "")
            data_s['strike_percentage'] = i['strikePercentage']
            data_s['strikeout_walk_ratio'] = i['strikeoutWalkRatio'].replace("-.--", "").replace(".---", "")
            data_s['strikeouts_per9'] = i['strikeoutsPer9'].replace("-.--", "").replace(".---", "")
            data_s['strikeouts_per_plate_appearance'] = i['strikeoutsPerPlateAppearance']
            data_s['strikes'] = i['strikes']
            data_s['swing_and_misses'] = i['swingAndMisses']
            data_s['total_bases'] = i['totalBases']
            data_s['total_swings'] = i['totalSwings']
            data_s['triples'] = i['triples']
            data_s['walks_per_plate_appearance'] = i['walksPerPlateAppearance']
            data_s['walks_per_strikeout'] = i['walksPerStrikeout'].replace("-.--", "").replace(".---", "")
            data_s['win_percentage'] = i['winPercentage'].replace("-.--", "").replace(".---", "")
            data_s['winning_percentage'] = i['winningPercentage'].replace("-.--", "").replace(".---", "")
            data_s['batters_faced'] = i['battersFaced']#.replace("-.--", "").replace(".---", "")
            data_s['number_of_pitches'] = i['numberOfPitches']
            data_s['pitches_per_inning'] = i['pitchesPerInning'].replace("-.--", "").replace(".---", "")
            data_s['quality_starts'] = i['qualityStarts']
            data_s['games_finished'] = i['gamesFinished']
            data_s['holds'] = i['holds']
            data_s['intentional_walks'] = i['intentionalWalks']
            data_s['wild_pitches'] = i['wildPitches']
            data_s['balks'] = i['balks']
            data_s['ground_into_double_play'] = i['groundIntoDoublePlay']
            data_s['ground_outs_to_airouts'] = i['groundOutsToAirouts'].replace("-.--", "").replace(".---", "")
            data_s['strikeouts_per9inn'] = i['strikeoutsPer9Inn'].replace("-.--", "").replace(".---", "")
            data_s['walks_per9inn'] = i['walksPer9Inn'].replace("-.--", "").replace(".---", "")
            data_s['strikesouts_to_walks'] = i['strikesoutsToWalks'].replace("-.--", "").replace(".---", "")
            data_s['babip'] = i['babip'].replace("-.--", "").replace(".---", "")
            data_s['stolen_bases'] = i['stolenBases']
            data_s['caught_stealing'] = i['caughtStealing']
            data_s['pickoffs'] = i['pickoffs'] 
            
            with write_lock:
                df = pd.DataFrame([data_s])
                # print(data_s)
                df.to_csv('./data/player_stats_pitching.csv', mode='a', header=not pd.io.common.file_exists('./data/player_stats_pitching.csv'), index=False)
            
            
            
            
    num_thread = 128
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_thread) as executor:
        print("crawl pitching")
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

    # Lấy list url
    list_url = [f"https://bdfed.stitch.mlbinfra.com/bdfed/stats/player?stitch_env=prod&season={i}&sportId=1&stats=season&group=hitting&gameType={t}&limit=1000&offset=0&sortStat=onBasePlusSlugging&order=desc" for i in range(START_YEAR, END_YEAR + 1) for t in ['R', 'P', 'F', 'D', 'L', 'W', 'A', 'S']]


    def get_player_hitting(url):
        response = requests.get(url)
        taget = response.json()
        year = url.split('&')[1].split('=')[1]
        group = url.split('&')[4].split('=')[1]
        session = url.split('&')[5].split('=')[1]
        
        session_mapping = {'R': 'regular_season', 'P': 'postseason', 'F': 'wild_card', 'D': 'division_series', 'L': 'league_championship_series', 'W': 'world_series', 'A': 'all_star_game', 'S': 'spring_training'}

        for i in taget['stats']:
            data_s = dict()
            
            data_s['year_game'] = i['year']
            data_s['type_game'] = session_mapping.get(session, session) 
            data_s['player_id'] = i['playerId']
            data_s['position'] = i['positionAbbrev']
            data_s['team_id'] = i['teamId']
            data_s['team_name'] = i['teamName']
            data_s['games_played'] = i['gamesPlayed']
            
            data_s['at_bats'] = i['atBats']
            data_s['runs'] = i['runs']
            data_s['hits'] = i['hits']
            data_s['doubles'] = i['doubles']
            data_s['triples'] = i['triples']
            data_s['home_runs'] = i['homeRuns']
            data_s['rbi'] = i['rbi']
            data_s['base_on_balls'] = i['baseOnBalls']
            data_s['strike_outs'] = i['strikeOuts']
            data_s['stolen_bases'] = i['stolenBases']
            data_s['caught_stealing'] = i['caughtStealing']
            data_s['avg'] = i['avg']
            data_s['obp'] = i['obp']
            data_s['slg'] = i['slg']
            data_s['ops'] = i['ops']
            data_s['left_on_base'] = i['leftOnBase']
            data_s['gidp'] = i['gidp']
            data_s['gidp_opp'] = i['gidpOpp']
            data_s['number_of_pitches'] = i['numberOfPitches']
            data_s['pitches_per_plate_appearance'] = i['pitchesPerPlateAppearance']
            data_s['home_runs_per_plate_appearance'] = i['homeRunsPerPlateAppearance']
            data_s['reached_on_error'] = i['reachedOnError']
            data_s['walk_offs'] = i['walkOffs']
            data_s['fly_outs'] = i['flyOuts']
            data_s['total_swings'] = i['totalSwings']
            data_s['swing_and_misses'] = i['swingAndMisses']
            data_s['balls_in_play'] = i['ballsInPlay']
            data_s['pop_outs'] = i['popOuts']
            data_s['line_outs'] = i['lineOuts']
            data_s['ground_outs'] = i['groundOuts']
            data_s['fly_hits'] = i['flyHits']
            data_s['pop_hits'] = i['popHits']
            data_s['line_hits'] = i['lineHits']
            data_s['ground_hits'] = i['groundHits']
            data_s['air_outs'] = i['airOuts']
            data_s['stolen_base_percentage'] = i['stolenBasePercentage'].replace(".---", "").replace("-.--", "")
            data_s['catchers_interference'] = i['catchersInterference']
            data_s['plate_appearances'] = i['plateAppearances']
            data_s['hit_by_pitch'] = i['hitByPitch']
            data_s['sac_bunts'] = i['sacBunts']
            data_s['sac_flies'] = i['sacFlies']
            data_s['ground_into_double_play'] = i['groundIntoDoublePlay']
            data_s['ground_outs_to_airouts'] = i['groundOutsToAirouts'].replace("-.--", "").replace(".---", "")
            data_s['extra_base_hits'] = i['extraBaseHits']
            data_s['total_bases'] = i['totalBases']
            data_s['intentional_walks'] = i['intentionalWalks']
            data_s['babip'] = i['babip'].replace("-.--", "").replace(".---", "")
            data_s['iso'] = i['iso'].replace("-.--", "").replace(".---", "")
            data_s['at_bats_per_home_run'] = i['atBatsPerHomeRun'].replace("-.--", "").replace(".---", "")
            data_s['walks_per_strikeout'] = i['walksPerStrikeout'].replace("-.--", "").replace(".---", "")
            data_s['walks_per_plate_appearance'] = i['walksPerPlateAppearance']
            data_s['strikeouts_per_plate_appearance'] = i['strikeoutsPerPlateAppearance']
            with write_lock:
                df = pd.DataFrame([data_s])
                df.to_csv('./data/player_stats_hitting.csv', mode='a', header=not pd.io.common.file_exists('./data/player_stats_hitting.csv'), index=False)
            
            
            
            
            
    num_thread = 128
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_thread) as executor:
        executor.map(get_player_hitting, list_url)
        print("crawl hitting")



    #________________________________________________________________________________________________end________________________________________________________________________________________________

