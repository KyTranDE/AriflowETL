import sys
sys.path.append('../')
from glob import glob
import json, os
import re
import pandas as pd
from threading import Thread
from tqdm import tqdm
from utils.utils import check_requests, write_log, save_json, write_csv, get_day, collect_data
from utils.logging import *
from config.teams import ID2TEAMNAME
from threading import Lock
write_lock = Lock()

#--- configs
url_day = 'https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate={date}&endDate={date}&gameType=E&&gameType=S&&gameType=R&&gameType=A&&gameType=F&&gameType=D&&gameType=L&&gameType=W&language=en&leagueId=103&&leagueId=104&hydrate=team,linescore,xrefId,flags,review,broadcasts(all),game(content(media(epg),summary),tickets),seriesStatus(useOverride=true),statusFlags&sortBy=gameDate,gameType,gameStatus&timeZone=America/New_York'
url_id = 'https://statsapi.mlb.com/api/v1.1/game/{id}/feed/live'

# gameData
INFO_GAME = {
    'game_id': 'game.pk', # mã trận đấu
    # 'game_number': 'game.gameNumber', # số trận đấu
    'season': 'game.season', # mùa giải
    'game_date': 'datetime.originalDate', # ngày thi đấu
    'day_night': 'datetime.dayNight', # buổi thi đấu
    'game_time': 'datetime.time', # thời gian thi đấu
    'ampm': 'datetime.ampm', # buổi thi đấu
}

# gameData.teams (home, away)
INFO_TEAM = {
    'team_id': 'id', # mã số đội chơi
    # 'team_name': 'name', # tên đội chơi
    'team_code': 'abbreviation', # mã đội chơi
    # 'team_code': 'teamCode', # mã đội chơi
    'short_name': 'shortName', # tên ngắn của đội chơi
}

# liveData.linescore.teams (home, away)
INFO_SCORE = {
    'runs': 'runs', # số điểm
    'hits': 'hits', # số lần đánh trúng bóng
    'errors': 'errors', # số lỗi
    'left_on_base': 'leftOnBase' # số người chơi còn lại
}

# gameData.teams (home, away) .record
INFO_RECORD = {
    'wins': 'teams.{team}.record.wins',
    'losses': 'teams.{team}.record.losses',
    'winning_pct': 'teams.{team}.record.winningPercentage'
}

# liveData.decisions (winner, loser, save)
INFO_HISTORY = {
    'player_id': '{dec}.id',
    'player_name': '{dec}.fullName',
}

# liveData.linescore
INFO_LINESCORE = {
    'balls': 'balls',
    'strikes': 'strikes',
    'outs': 'outs'
}

# INFO_BATTING = {
#     'fly_outs': 'flyOuts',
#     'ground_outs': 'groundOuts',
#     'runs': 'runs',
#     'doubles': 'doubles',
#     'triples': 'triples',
#     'home_runs': 'homeRuns',
#     'strike_outs': 'strikeOuts',
#     'base_on_balls': 'baseOnBalls',
#     'intentional_walks': 'intentionalWalks',
#     'hits': 'hits',
#     'hit_by_pitch': 'hitByPitch',
#     'avg': 'avg',
#     'at_bats': 'atBats',
#     'obp': 'obp',
#     'slg': 'slg',
#     'ops': 'ops',
#     'caught_stealing': 'caughtStealing',
#     'stolen_bases': 'stolenBases',
#     'stolen_base_percentage': 'stolenBasePercentage',
#     'ground_into_double_play': 'groundIntoDoublePlay',
#     'ground_into_triple_play': 'groundIntoTriplePlay',
#     'plate_appearances': 'plateAppearances',
#     'total_bases': 'totalBases',
#     'rbi': 'rbi',
#     'left_on_base': 'leftOnBase',
#     'sac_bunts': 'sacBunts',
#     'sac_flies': 'sacFlies',
#     'catchers_interference': 'catchersInterference',
#     'pickoffs': 'pickoffs',
#     'at_bats_per_home_run': 'atBatsPerHomeRun'
# }
INFO_BATTING_TEAM = {
    "at_bats": "atBats",
    "at_bats_per_home_run": "atBatsPerHomeRun",
    "avg": "avg",
    "base_on_balls": "baseOnBalls",
    "catchers_interference": "catchersInterference",
    "caught_stealing": "caughtStealing",
    "doubles": "doubles",
    "fly_outs": "flyOuts",
    "ground_into_double_play": "groundIntoDoublePlay",
    "ground_into_triple_play": "groundIntoTriplePlay",
    "ground_outs": "groundOuts",
    "hit_by_pitch": "hitByPitch",
    "hits": "hits",
    "home_runs": "homeRuns",
    "intentional_walks": "intentionalWalks",
    "left_on_base": "leftOnBase",
    "obp": "obp",
    "ops": "ops",
    "pickoffs": "pickoffs",
    "plate_appearances": "plateAppearances",
    "rbi": "rbi",
    "runs": "runs",
    "sac_bunts": "sacBunts",
    "sac_flies": "sacFlies",
    "slg": "slg",
    "stolen_base_percentage": "stolenBasePercentage",
    "stolen_bases": "stolenBases",
    "strike_outs": "strikeOuts",
    "total_bases": "totalBases",
    "triples": "triples"
}

# INFO_PITCHING = {
#     'ground_outs': 'groundOuts',
#     'air_outs': 'airOuts',
#     'runs': 'runs',
#     'doubles': 'doubles',
#     'triples': 'triples',
#     'home_runs': 'homeRuns',
#     'strike_outs': 'strikeOuts',
#     'base_on_balls': 'baseOnBalls',
#     'intentional_walks': 'intentionalWalks',
#     'hits': 'hits',
#     'hit_by_pitch': 'hitByPitch',
#     'at_bats': 'atBats',
#     'obp': 'obp',
#     'caught_stealing': 'caughtStealing',
#     'stolen_bases': 'stolenBases',
#     'stolen_base_percentage': 'stolenBasePercentage',
#     'number_of_pitches': 'numberOfPitches',
#     'era': 'era',
#     'innings_pitched': 'inningsPitched',
#     'save_opportunities': 'saveOpportunities',
#     'earned_runs': 'earnedRuns',
#     'whip': 'whip',
#     'batters_faced': 'battersFaced',
#     'outs': 'outs',
#     'complete_games': 'completeGames',
#     'shutouts': 'shutouts',
#     'pitches_thrown': 'pitchesThrown',
#     'balls': 'balls',
#     'strikes': 'strikes',
#     'strike_percentage': 'strikePercentage',
#     'hit_batsmen': 'hitBatsmen',
#     'balks': 'balks',
#     'wild_pitches': 'wildPitches',
#     'pickoffs': 'pickoffs',
#     'ground_outs_to_airouts': 'groundOutsToAirouts',
#     'rbi': 'rbi',
#     'pitches_per_inning': 'pitchesPerInning',
#     'runs_scored_per9': 'runsScoredPer9',
#     'home_runs_per9': 'homeRunsPer9',
#     'inherited_runners': 'inheritedRunners',
#     'inherited_runners_scored': 'inheritedRunnersScored',
#     'catchers_interference': 'catchersInterference',
#     'sac_bunts': 'sacBunts',
#     'sac_flies': 'sacFlies',
#     'passed_ball': 'passedBall'
# }
INFO_PITCHING_TEAM = {
    "air_outs": "airOuts",
    "at_bats": "atBats",
    "balks": "balks",
    "balls": "balls",
    "base_on_balls": "baseOnBalls",
    "batters_faced": "battersFaced",
    "catchers_interference": "catchersInterference",
    "caught_stealing": "caughtStealing",
    "complete_games": "completeGames",
    "doubles": "doubles",
    "earned_runs": "earnedRuns",
    "era": "era",
    "ground_outs": "groundOuts",
    "ground_outs_to_airouts": "groundOutsToAirouts",
    "hit_batsmen": "hitBatsmen",
    "hit_by_pitch": "hitByPitch",
    "hits": "hits",
    "home_runs": "homeRuns",
    "home_runs_per9": "homeRunsPer9",
    "inherited_runners": "inheritedRunners",
    "inherited_runners_scored": "inheritedRunnersScored",
    "innings_pitched": "inningsPitched",
    "intentional_walks": "intentionalWalks",
    "number_of_pitches": "numberOfPitches",
    "obp": "obp",
    "outs": "outs",
    "passed_ball": "passedBall",
    "pickoffs": "pickoffs",
    "pitches_per_inning": "pitchesPerInning",
    "pitches_thrown": "pitchesThrown",
    "rbi": "rbi",
    "runs": "runs",
    "runs_scored_per9": "runsScoredPer9",
    "sac_bunts": "sacBunts",
    "sac_flies": "sacFlies",
    "save_opportunities": "saveOpportunities",
    "shutouts": "shutouts",
    "stolen_base_percentage": "stolenBasePercentage",
    "stolen_bases": "stolenBases",
    "strike_outs": "strikeOuts",
    "strike_percentage": "strikePercentage",
    "strikes": "strikes",
    "triples": "triples",
    "whip": "whip",
    "wild_pitches": "wildPitches"
}

# INFO_FIELDING = {
#     'caught_stealing': 'caughtStealing',
#     'stolen_bases': 'stolenBases',
#     'stolen_base_percentage': 'stolenBasePercentage',
#     'assists': 'assists',
#     'put_outs': 'putOuts',
#     'errors': 'errors',
#     'chances': 'chances',
#     'passed_ball': 'passedBall',
#     'pickoffs': 'pickoffs'
# }
INFO_FIELDING_TEAM = {
    "assists": "assists",
    "caught_stealing": "caughtStealing",
    "chances": "chances",
    "errors": "errors",
    "passed_ball": "passedBall",
    "pickoffs": "pickoffs",
    "put_outs": "putOuts",
    "stolen_base_percentage": "stolenBasePercentage",
    "stolen_bases": "stolenBases"
}

# liveData.boxscore.teams.(home, away).players.(player_id)
INFO_BATTING_PLAYER = {
    'player_id': 'person.id',
    'jersey_number': 'jerseyNumber',
    'position': 'position.abbreviation',
    'summary': 'stats.batting.summary',
    'fly_outs': 'stats.batting.flyOuts',
    'ground_outs': 'stats.batting.groundOuts',
    'runs': 'stats.batting.runs',
    'doubles': 'stats.batting.doubles',
    'triples': 'stats.batting.triples',
    'home_runs': 'stats.batting.homeRuns',
    'strike_outs': 'stats.batting.strikeOuts',
    'base_on_balls': 'stats.batting.baseOnBalls',
    'intentional_walks': 'stats.batting.intentionalWalks',
    'hits': 'stats.batting.hits',
    'hit_by_pitch': 'stats.batting.hitByPitch',
    'at_bats': 'stats.batting.atBats',
    'caught_stealing': 'stats.batting.caughtStealing',
    'stolen_bases': 'stats.batting.stolenBases',
    'ground_into_double_play': 'stats.batting.groundIntoDoublePlay',
    'ground_into_triple_play': 'stats.batting.groundIntoTriplePlay',
    'plate_appearances': 'stats.batting.plateAppearances',
    'total_bases': 'stats.batting.totalBases',
    'rbi': 'stats.batting.rbi',
    'left_on_base': 'stats.batting.leftOnBase',
    'sac_bunts': 'stats.batting.sacBunts',
    'sac_flies': 'stats.batting.sacFlies',
    'catchers_interference': 'stats.batting.catchersInterference',
    'pickoffs': 'stats.batting.pickoffs'
}

INFO_PITCHING_PLAYER = {
    'player_id': 'person.id',
    'jersey_number': 'jerseyNumber',
    'position': 'position.abbreviation',
    'summary': 'stats.pitching.summary',
    'fly_outs': 'stats.pitching.flyOuts',
    'ground_outs': 'stats.pitching.groundOuts',
    'air_outs': 'stats.pitching.airOuts',
    'runs': 'stats.pitching.runs',
    'doubles': 'stats.pitching.doubles',
    'triples': 'stats.pitching.triples',
    'home_runs': 'stats.pitching.homeRuns',
    'strike_outs': 'stats.pitching.strikeOuts',
    'base_on_balls': 'stats.pitching.baseOnBalls',
    'intentional_walks': 'stats.pitching.intentionalWalks',
    'hits': 'stats.pitching.hits',
    'hit_by_pitch': 'stats.pitching.hitByPitch',
    'at_bats': 'stats.pitching.atBats',
    'obp': 'stats.pitching.obp',
    'caught_stealing': 'stats.pitching.caughtStealing',
    'stolen_bases': 'stats.pitching.stolenBases',
    'number_of_pitches': 'stats.pitching.numberOfPitches',
    'era': 'stats.pitching.era',
    'innings_pitched': 'stats.pitching.inningsPitched',
    'wins': 'stats.pitching.wins',
    'losses': 'stats.pitching.losses',
    'saves': 'stats.pitching.saves',
    'save_opportunities': 'stats.pitching.saveOpportunities',
    'holds': 'stats.pitching.holds',
    'blown_saves': 'stats.pitching.blownSaves',
    'earned_runs': 'stats.pitching.earnedRuns',
    'whip': 'stats.pitching.whip',
    'batters_faced': 'stats.pitching.battersFaced',
    'outs': 'stats.pitching.outs',
    'games_pitched': 'stats.pitching.gamesPitched',
    'complete_games': 'stats.pitching.completeGames',
    'shutouts': 'stats.pitching.shutouts',
    'balls': 'stats.pitching.balls',
    'strikes': 'stats.pitching.strikes',
    'strike_percentage': 'stats.pitching.strikePercentage',
    'hit_batsmen': 'stats.pitching.hitBatsmen',
    'balks': 'stats.pitching.balks',
    'wild_pitches': 'stats.pitching.wildPitches',
    'pickoffs': 'stats.pitching.pickoffs',
    'ground_outs_to_airouts': 'stats.pitching.groundOutsToAirouts',
    'rbi': 'stats.pitching.rbi',
    'win_percentage': 'stats.pitching.winPercentage',
    'pitches_per_inning': 'stats.pitching.pitchesPerInning',
    'pitches_thrown': 'pitchesThrown',
    'games_finished': 'stats.pitching.gamesFinished',
    'strikeout_walk_ratio': 'stats.pitching.strikeoutWalkRatio',
    'strikeouts_per9inn': 'stats.pitching.strikeoutsPer9Inn',
    'walks_per9inn': 'stats.pitching.walksPer9Inn',
    'hits_per9inn': 'stats.pitching.hitsPer9Inn',
    'runs_scored_per9': 'stats.pitching.runsScoredPer9',
    'home_runs_per9': 'stats.pitching.homeRunsPer9',
    'inherited_runners': 'stats.pitching.inheritedRunners',
    'inherited_runners_scored': 'stats.pitching.inheritedRunnersScored',
    'catchers_interference': 'stats.pitching.catchersInterference',
    'sac_bunts': 'stats.pitching.sacBunts',
    'sac_flies': 'stats.pitching.sacFlies',
    'passed_ball': 'stats.pitching.passedBall'
}

INFO_FIELDING_PLAYER = {
    'player_id': 'person.id',
    'jersey_number': 'jerseyNumber',
    'position': 'position.abbreviation',
    'caught_stealing': 'stats.fielding.caughtStealing',
    'stolen_bases': 'stats.fielding.stolenBases',
    'assists': 'stats.fielding.assists',
    'put_outs': 'stats.fielding.putOuts',
    'errors': 'stats.fielding.errors',
    'chances': 'stats.fielding.chances',
    'fielding': 'stats.fielding.fielding',
    'passed_ball': 'stats.fielding.passedBall',
    'pickoffs': 'stats.fielding.pickoffs'
}

#------------------------------------------------------------

def get_data_from_day(date):
    response = check_requests(url_day.format(date=date))
    if not response:
        return None
    return response.json()

def get_data_from_id(id):
    response = check_requests(url_id.format(id=id))
    if not response:
        return None
    return response.json()
    
def crawl(year):
    timelines = get_day(year)
    n = len(timelines)
    for idx, date in enumerate(timelines):
        info = get_data_from_day(date)
        # print(info)
        if info:
            if info['totalGames'] == 0:
                logger('success', f'{date} {status(idx, n)}: 0 battles')
                write_log('../log/score.log', f'[Success] {date} {status(idx, n)}:0 battles')
                continue
            info = info['dates'][0]
            # save data each battle
            ids = [game['gamePk'] for game in info['games']]
            for id in ids:
                data_id = get_data_from_id(id)
                save_json(data_id, f'../data/crawl/score/{date}/{id}.json')
            # save data each day
            save_json(info, f'../data/crawl/score/{date}/gameday.json')
            write_log('../log/score.log', f'[Success] {date}: {len(ids)} battles')
            logger('success', f'{date} {status(idx, n)}: {len(ids)} battles')
        else:
            logger('error', f'{date} {status(idx, n)}')
            write_log('../log/score.log', f'[Error] {date}')

def process_team(team, state):
    try:
        dict_team = {}
        for key in INFO_TEAM:
            dict_team[key] = [collect_data(team, INFO_TEAM[key])]
        if dict_team['team_id'][0] in ID2TEAMNAME.keys():
            dict_team['team_name'] = [ID2TEAMNAME[dict_team['team_id'][0]]]
            return dict_team['team_id'][0]
        return None
        # print(dict_team)
        # df = pd.DataFrame(dict_team)
        # try:
        #     df_old = pd.read_csv('../data/process/team.csv')
        #     df = pd.concat([df_old, df], axis=0)
        # except:
        #     pass
        # df = df.drop_duplicates().reset_index(drop=True)
        # df.to_csv('../data/crawl/team.csv', index=False)
        # write_log(f'../log/process_team.log', f'{state}: SUSSES')
        

    except Exception as e:
        write_log(f'../log/process_team.log', f'{e}')
        return None

def process_game(gameData, state):
    try:
        dict_game = {}
        # GAME TABLE
        for key in INFO_GAME:
            dict_game[key] = collect_data(gameData, INFO_GAME[key])
        for team in ['home', 'away']:
            dict_game[f'{team}_team_id'] = process_team(gameData['teams'][team], state) #[collect_data(gameData['teams'][team], INFO_TEAM['team_id'])]
            # if dict_game[f'{team}_team_id'] == None:
            #     return None
        # if dict_game['home_team_id'] in ID2TEAMNAME.keys() and dict_game['away_team_id'] in ID2TEAMNAME.keys():
            # write_csv(f'../data/process/game.csv', dict_game)
            # write_log(f'../log/process_game.log', f'{state}: SUSSES')
            return dict_game['game_id']
        return None
    except Exception as e:
        write_log(f'../log/process_game.log', f'{state}: {e}')
        return None

def process_linescore(game_id, liveData, state):
    dict_linescore = {}
    dict_linescore['game_id'] = game_id
    try:
        dict_linescore['innings'] = 0
        for key in INFO_SCORE:
            for team in ['home', 'away']:
                dict_linescore[f'{team}_{key}'] = collect_data(liveData['linescore']['teams'][team], INFO_SCORE[key])
        write_csv(f'../data/process/linescore.csv', dict_linescore)
        for idx, dict_inning in enumerate(liveData['linescore']['innings'], 1):
            dict_linescore['innings'] = idx
            for key in INFO_SCORE:
                for team in ['home', 'away']:
                    dict_linescore[f'{team}_{key}'] = collect_data(dict_inning[team], INFO_SCORE[key])
            write_csv(f'../data/process/linescore.csv', dict_linescore)
        # for key in INFO_LINESCORE:
        #     dict_linescore[key] = collect_data(liveData['linescore'], INFO_LINESCORE[key])
        
        # write_log(f'../log/process_linescore.log', f'{state}: SUSSES')
    except Exception as e:
        write_log(f'../log/process_linescore.log', f'{state}: {e}')

def process_history(game_id, liveData, gameData, state):
    dict_history = {}
    dict_history['game_id'] = game_id
    try:
        for key in INFO_HISTORY:
            for dec in ['winner', 'loser', 'save']:
                dict_history[f'{key}_{dec}'] = collect_data(liveData['decisions'], INFO_HISTORY[key].format(dec=dec))
        for key in INFO_RECORD:
            for team in ['home', 'away']:
                dict_history[f'{team}_{key}'] = collect_data(gameData, INFO_RECORD[key].format(team=team))
        write_csv(f'../data/process/history.csv', dict_history)
        # write_log(f'../log/process_history.log', f'{state}: SUSSES')
    except Exception as e:
        write_log(f'../log/process_history.log', f'{state}: {e}')

def process_batting_team(game_id, team_id, dict_batting, state):
    if dict_batting == {}:
        return None
    res = {}
    res['game_id'] = game_id
    res['team_id'] = team_id
    try:
        for key in INFO_BATTING_TEAM:
            res[key] = collect_data(dict_batting, INFO_BATTING_TEAM[key])
        write_csv(f'../data/process/boxscore_batting_team.csv', res)
        # write_log(f'../log/process_boxscore_batting_team.log', f'{state}: SUSSES')
    except Exception as e:
        write_log(f'../log/process_boxscore_batting_team.log', f'{state}: {e}')
        
def process_pitching_team(game_id, team_id, dict_pitching, state):
    if dict_pitching == {}:
        return None
    res = {}
    res['game_id'] = game_id
    res['team_id'] = team_id
    try:
        for key in INFO_PITCHING_TEAM:
            res[key] = collect_data(dict_pitching, INFO_PITCHING_TEAM[key])
        write_csv(f'../data/process/boxscore_pitching_team.csv', res)
        # write_log(f'../log/process_boxscore_pitching_team.log', f'{state}: SUSSES')
    except Exception as e:
        write_log(f'../log/process_boxscore_pitching_team.log', f'{state}: {e}')

def process_fielding_team(game_id, team_id, dict_fielding, state):
    if dict_fielding == {}:
        return None
    res = {}
    res['game_id'] = game_id
    res['team_id'] = team_id
    try:
        for key in INFO_FIELDING_TEAM:
            res[key] = collect_data(dict_fielding, INFO_FIELDING_TEAM[key])
        write_csv(f'../data/process/boxscore_fielding_team.csv', res)
        # write_log(f'../log/process_boxscore_fielding_team.log', f'{state}: SUSSES')
    except Exception as e:
        write_log(f'../log/process_boxscore_fielding_team.log', f'{state}: {e}')
    
def process_batting_player(game_id, team_id, dict_batting, state):
    res = {}
    res['game_id'] = game_id
    res['team_id'] = team_id
    if dict_batting['stats']['batting'] == {}:
        return
    try:
        for key in INFO_BATTING_PLAYER:
            res[key] = collect_data(dict_batting, INFO_BATTING_PLAYER[key])
        write_csv(f'../data/process/boxscore_batting_player.csv', res)
        # write_log(f'../log/process_boxscore_batting_player.log', f'{state}: SUSSES')
    except Exception as e:
        write_log(f'../log/process_boxscore_batting_player.log', f'{state}: {e}')
        
def process_pitching_player(game_id, team_id, dict_pitching, state):
    res = {}
    res['game_id'] = game_id
    res['team_id'] = team_id
    if dict_pitching['stats']['pitching'] == {}:
        return
    try:
        for key in INFO_PITCHING_PLAYER:
            res[key] = collect_data(dict_pitching, INFO_PITCHING_PLAYER[key])
        write_csv(f'../data/process/boxscore_pitching_player.csv', res)
        # write_log(f'../log/process_boxscore_pitching_player.log', f'{state}: SUSSES')
    except Exception as e:
        write_log(f'../log/process_boxscore_pitching_player.log', f'{state}: {e}')
        
def process_fielding_player(game_id, team_id, dict_fielding, state):
    res = {}
    res['game_id'] = game_id
    res['team_id'] = team_id
    if dict_fielding['stats']['fielding'] == {}:
        return
    try:
        for key in INFO_FIELDING_PLAYER:
            res[key] = collect_data(dict_fielding, INFO_FIELDING_PLAYER[key])
        write_csv(f'../data/process/boxscore_fielding_player.csv', res)
        # write_log(f'../log/process_boxscore_fielding_player.log', f'{state}: SUSSES')
    except Exception as e:
        write_log(f'../log/process_boxscore_fielding_player.log', f'{state}: {e}')

# def run(start_season=2013, end_season=2023):
#     try:
#         for s in range(start_season, end_season+1):
#             t = Thread(target=crawl, args=(s,))
#             t.start()
#     except:
#         print ("error")

def fetch_data(year):
    files = glob(f'../data/crawl/score/{year}*/*[0-9].json')[:100]
    loop = tqdm(files, desc=f'Score {year}', colour='green')
    for battle in loop:
        loop.set_postfix(id = OK(battle.split('\\')[-1].split('.')[0]))
        with open(battle, 'r+', encoding='utf-8') as f:
            try:
                json_data = json.load(f)
                gameData = json_data['gameData']
                liveData = json_data['liveData']
                anno_time = ' '.join(battle.split('\\')[-2:]).split('.')[0]
                game_id = process_game(gameData, anno_time)
                if not game_id:
                    continue
                process_linescore(game_id, liveData, anno_time)
                process_history(game_id, liveData, gameData, anno_time)
                for team in ['home', 'away']:
                    team_id = liveData['boxscore']['teams'][team]['team']['id']
                    process_batting_team(game_id, team_id, liveData['boxscore']['teams'][team]['teamStats']['batting'], anno_time)
                    process_pitching_team(game_id, team_id, liveData['boxscore']['teams'][team]['teamStats']['pitching'], anno_time)
                    process_fielding_team(game_id, team_id, liveData['boxscore']['teams'][team]['teamStats']['fielding'], anno_time)
                    player_dicts = liveData['boxscore']['teams'][team]['players']
                    for player_key in player_dicts:
                        process_batting_player(game_id, team_id, player_dicts[player_key], anno_time)
                        process_pitching_player(game_id, team_id, player_dicts[player_key], anno_time)
                        process_fielding_player(game_id, team_id, player_dicts[player_key], anno_time)
                    del player_dicts
            except:
                continue

if __name__ == '__main__':
    for year in range(2023, 2024):
        fetch_data(year)