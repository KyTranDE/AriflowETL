def injury_info():
    import pandas as pd
    import requests
    from bs4 import BeautifulSoup

    from MLB.config.teams import TEAMNAME2ID
    headers = {
        'authority': 'www.espn.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en,vi;q=0.9',
        'cache-control': 'max-age=0',
        # 'cookie': 'SWID=37EAEA10-4888-4FB9-CFEF-A29CA7564A0A; s_ecid=MCMID%7C39210382655858214673123392917879020172; _cb_ls=1; _v__chartbeat3=CLjwlYB4rY7xCz1gNb; __gads=ID=af7c35fe92d01a1a:T=1698223681:RT=1698223681:S=ALNI_MZDKmnyxS4Em7u3zvg921W96BYomA; __gpi=UID=00000c734726bbc8:T=1698223681:RT=1698223681:S=ALNI_MZpw3YRSoBAzNxN3ZO6uetIZo9M8A; _gcl_au=1.1.996170328.1698223684; _cb=Bw8kSUBOJmhjD1GhXy; _fbp=fb.1.1698223684764.1915055393; s_vi=[CS]v1|32A0DFABF68CC159-60000036E0B731E4[CE]; s_pers=%20s_c24%3D1699444549661%7C1794052549661%3B%20s_c24_s%3DLess%2520than%25207%2520days%7C1699446349661%3B%20s_gpv_pn%3Despn%253Anfl%253Aplayers%7C1699446349667%3B; _chartbeat2=.1695701421415.1699477024564.1011111101000011.tasPU9ACONC0buUkBVVu-DnQ7PA.8; nol_fpid=sr2z4gungq0e8vfy8niebmyobpzvh1698223684|1698223684624|1699477025906|1699477025909; _cb=Bw8kSUBOJmhjD1GhXy; _chartbeat2=.1695701421415.1699494341151.1011111101000011.C0W33HBhYmhABxcGR8D7vb4_DKKXp5.1; edition=espn-en-us; connectionspeed=data-lite; edition-view=espn-en-us; region=unknown; _dcf=1; country=vn; _nr=0; s_ensCDS=0; check=true; AMCVS_EE0201AC512D2BE80A490D4C%40AdobeOrg=1; AMCV_EE0201AC512D2BE80A490D4C%40AdobeOrg=-330454231%7CMCIDTS%7C19690%7CMCMID%7C39210382655858214673123392917879020172%7CMCAAMLH-1700081294%7C3%7CMCAAMB-1701141027%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1701160606s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C3.1.2; s_cc=true; mbox=PC#4be2cd9abf8e4c2080eeb080c3104611.38_0#1762721333|session#c42798e99ee049769e4d30f0828a755a#1701157643; s_ensNR=1701155782673-Repeat; s_c24_s=Less%20than%201%20day; s_gpv_pn=espn%3A%3Ainjuries; s_c6=1701155783837-Repeat; s_c24=1701155786628',
        'if-modified-since': 'Tue, 28 Nov 2023 07:16:53 GMT',
        'referer': 'https://www.google.com/',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
    }
    response = requests.get('https://www.espn.com/mlb/injuries', headers=headers)

    soup = BeautifulSoup(response.content, 'html.parser')
    taget = soup.find_all('div', class_='ResponsiveTable Table__league-injuries')
    try:
        for j in taget:
            team_name = j.find('span', class_='injuries__teamName ml2').text
            tg = ((j.find_all("tr",class_="Table__TR Table__TR--sm Table__even")))
            for i  in tg:
                data = dict()
                data["player_id"] = (i.find_all('td')[0].find('a')['href'].split('/')[-1])
                data["player_name"] = (i.find_all('td')[0].text)
                data["team_name"] = team_name
                data["team_id"]= TEAMNAME2ID.get(team_name,team_name)
                data["pos"] = (i.find_all('td')[1].text)
                data["date"] = (i.find_all('td')[2].text)
                data["status"]= (i.find_all('td')[3].text)
                data["comment"] = (i.find_all('td')[4].text)
                df = pd.DataFrame([data])
                df.to_csv('../data/process/injury_info.csv', mode='a', header=not pd.io.common.file_exists('../data/process/injury_info.csv'), index=False)
    except Exception as e:
        print(e)






