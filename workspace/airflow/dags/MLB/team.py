
def team() :
    from bs4 import BeautifulSoup
    import requests
    import re
    import pandas as pd
    from tqdm import tqdm
    tqdm.pandas(desc="progress")
    from MLB.config.teams import TEAMNAME2ID

    def get_link(url):
        team_url = []
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        taget = soup.find('tbody')
        for i in taget.find_all('tr'):
            try:
                team_url.append('https://www.baseball-reference.com' + i.find('td', class_='left').find('a')['href'])
                
            except:
                pass
        return team_url

    def get_data(url):
        team_info = []
        data = dict()
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            taget = soup.find('div', {'id' : 'meta'})
            data['team_name'] = taget.find('h1').span.text
            data['seasons'] = taget.find_all('p')[3].text.replace(taget.find_all('p')[3].strong.text, '').replace("\n",'').strip().split(' ')[0]
            data['wins'] = taget.find_all('p')[4].text.replace(taget.find_all('p')[4].strong.text, '').replace("\n",'').strip().split('-')[0]
            data['losses'] = taget.find_all('p')[4].text.replace(taget.find_all('p')[4].strong.text, '').replace("\n",'').strip().split(',')[0].split('-')[1]
            data['playoff_appearances'] = taget.find_all('p')[5].text.replace(taget.find_all('p')[5].strong.text, '').replace("\n",'').strip()
            data['pennants'] = taget.find_all('p')[6].text.replace(taget.find_all('p')[6].strong.text, '').replace("\n",'').strip()
            data['world_championships'] = taget.find_all('p')[7].text.replace(taget.find_all('p')[7].strong.text, '').replace("\n",'').strip()
            data['winningest_manager'] = taget.find_all('p')[8].text.replace(taget.find_all('p')[8].strong.text, '').replace("\n",'').strip().split(',')[0]
            
        except:
            data['team_name'] = taget.find('h1').span.text
            data['seasons'] = taget.find_all('p')[4].text.replace(taget.find_all('p')[4].strong.text, '').replace("\n",'').strip().split(' ')[0]
            data['wins'] = taget.find_all('p')[5].text.replace(taget.find_all('p')[5].strong.text, '').replace("\n",'').strip().split('-')[0]
            data['losses'] = taget.find_all('p')[5].text.replace(taget.find_all('p')[5].strong.text, '').replace("\n",'').strip().split(',')[0].split('-')[1]
            data['playoff_appearances'] = taget.find_all('p')[6].text.replace(taget.find_all('p')[6].strong.text, '').replace("\n",'').strip()
            data['pennants'] = taget.find_all('p')[7].text.replace(taget.find_all('p')[7].strong.text, '').replace("\n",'').strip()
            data['world_championships'] = taget.find_all('p')[8].text.replace(taget.find_all('p')[8].strong.text, '').replace("\n",'').strip()
            data['winningest_manager'] = taget.find_all('p')[9].text.replace(taget.find_all('p')[9].strong.text, '').replace("\n",'').strip().split(',')[0]
            print(1)
        team_info.append(data)
        return team_info


    team_url = get_link('https://www.baseball-reference.com/teams/')
    team_info = []
    for i in tqdm(team_url):
        team_info.extend(get_data(i))
    df = pd.DataFrame(team_info)
    print("_________________________________________________________")
    df.to_csv('./data/process/team_info.csv', index=False)
        
    def process():
        df_team_in4 = pd.read_csv('.//data//prosess//team_info.csv')
        df_team = pd.read_csv('.//data//prosess//team.csv')
        
        df_team_in4['team_id'] = df_team_in4['team_name'].progress_apply(lambda x: TEAMNAME2ID[x])
        df = pd.merge(df_team, df_team_in4[['team_id', 'seasons', 'wins', 'losses', 'playoff_appearances', 'pennants', 'world_championships']], on='team_id', how='left')
        df.to_csv('./data/team.csv', index=False)

    process()