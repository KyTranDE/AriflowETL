def rank_team():
    TEAMNAME2ID = {
        'Atlanta Braves': 144,
        'Texas Rangers': 140,
        'Seattle Mariners': 136,
        'San Francisco Giants': 137,
        'Los Angeles Dodgers': 119,
        'Los Angeles Angels': 108,
        'Cincinnati Reds': 113,
        'Arizona Diamondbacks': 109,
        'Philadelphia Phillies': 143,
        'Baltimore Orioles': 110,
        'Milwaukee Brewers': 158,
        'Tampa Bay Rays': 139,
        'San Diego Padres': 135,
        'Miami Marlins': 146,
        'Boston Red Sox': 111,
        'Kansas City Royals': 118,
        'Detroit Tigers': 116,
        'New York Mets': 121,
        'Colorado Rockies': 115,
        'Pittsburgh Pirates': 134,
        'Toronto Blue Jays': 141,
        'St. Louis Cardinals': 138,
        'Chicago White Sox': 145,
        'Washington Nationals': 120,
        'Houston Astros': 117,
        'Chicago Cubs': 112,
        'Minnesota Twins': 142,
        'New York Yankees': 147,
        'Oakland Athletics': 133,
        'Cleveland Guardians': 114,
        'Cleveland Indians': 114,
    }




    #________________________________________________________________________________________________________________________STANDINGS OVERALL-LEAGUE-DIVISION________________________________________________________________________________________________________________________

    import pandas as pd
    import requests
    from bs4 import BeautifulSoup
    from concurrent.futures import ThreadPoolExecutor
    from threading import Lock
    from MLB.config.params import START_YEAR, END_YEAR
    write_lock = Lock()
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

        

    def get_data(url):
        try:
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            tagert = (soup.find_all("tbody", class_="Table__TBODY")[1])
            count = 0
            for i in tagert.find_all("tr"):
                data = dict()
                data["year_game"] = url.split("/")[7]
                data["type_rank"] = "overall"
                data["team_id"] = TEAMNAME2ID.get((soup.find_all('span', class_='hide-mobile')[count].text),(soup.find_all('span', class_='hide-mobile')[count].text))
                data["num_rank"] = count + 1
                data["area"] = soup.find("div",class_="Table__Title").text            
                data["win"] = (i.find_all("td")[0].text)
                data["losses"]  = (i.find_all("td")[1].text)
                data["winning_percentage"] = (i.find_all("td")[2].text)
                data["games_back"] = (i.find_all("td")[3].text).replace("-","")
                data["home_win"] = (i.find_all("td")[4].text).split("-")[0]
                data["home_loss"] = (i.find_all("td")[4].text).split("-")[1]
                data["away_win"] = (i.find_all("td")[5].text). split("-")[0]
                data["away_loss"] = (i.find_all("td")[5].text).split("-")[1]
                data["rs"] = (i.find_all("td")[6].text)
                data["ra"] = (i.find_all("td")[7].text)
                data["diff"] = (i.find_all("td")[8].text)
                data["strk"] = (i.find_all("td")[9].text) 
                data["l10_win"] = (i.find_all("td")[10].text).split("-")[0]
                data["l10_loss"] = (i.find_all("td")[10].text).split("-")[1]
                td_elements = i.find_all("td")
                if len(td_elements) > 11:
                    data["poff"] = td_elements[11].text
                else:
                    data["poff"] = ""
                with write_lock:
                    df = pd.DataFrame([data])
                    df.to_csv('./data/rank_team_standings.csv', mode='a', header=not pd.io.common.file_exists('./data/rank_team_standing.csv'), index=False)

                count += 1
        except Exception as e:
            print(e)
            pass

    list_url = [f"https://www.espn.com/mlb/standings/_/season/{i}/group/overall" for i in range(START_YEAR, END_YEAR+1)]

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(get_data, list_url)



    import pandas as pd
    import requests
    from bs4 import BeautifulSoup
    from concurrent.futures import ThreadPoolExecutor
    from threading import Lock

    write_lock = Lock()
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
    def get_data(url):
        try:
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            tagert = (soup.find_all("tbody", class_="Table__TBODY")[1])
            area = soup.find_all("div",class_="Table__Title")
            count = 0
            for i in tagert.find_all("tr"):
                data = dict()
                data["team_id"] = TEAMNAME2ID.get((soup.find_all('span', class_='hide-mobile')[count].text),(soup.find_all('span', class_='hide-mobile')[count].text))
                data["type_rank"] = "league"
                data["num_rank"] = count + 1
                data["area"] = area[0].text
                data["year_game"] = url.split("/")[7]
                data["win"] = (i.find_all("td")[0].text)
                data["losses"]  = (i.find_all("td")[1].text)
                data["winning_percentage"] = (i.find_all("td")[2].text)
                data["games_back"] = (i.find_all("td")[3].text)
                data["home_win"] = (i.find_all("td")[4].text).split("-")[0]
                data["home_loss"] = (i.find_all("td")[4].text).split("-")[1]
                data["away_win"] = (i.find_all("td")[5].text). split("-")[0]
                data["away_loss"] = (i.find_all("td")[5].text).split("-")[1]
                data["rs"] = (i.find_all("td")[6].text)
                data["ra"] = (i.find_all("td")[7].text)
                data["diff"] = (i.find_all("td")[8].text)
                data["strk"] = (i.find_all("td")[9].text)
                data["l10_win"] = (i.find_all("td")[10].text).split("-")[0]
                data["l10_loss"] = (i.find_all("td")[10].text).split("-")[1]
                td_elements = i.find_all("td") 
                if len(td_elements) > 11:
                    data["poff"] = td_elements[11].text
                else:
                    data["poff"] = ""
                with write_lock:
                    df = pd.DataFrame([data])
                    df.to_csv('./data/rank_team_standing.csv', mode='a', header=not pd.io.common.file_exists('./data/rank_team_standing.csv'), index=False)

                count += 1
                
            tagert = (soup.find_all("tbody", class_="Table__TBODY")[3])
            count = 15
            for i in tagert.find_all("tr"):
                data = dict()
                data["team_id"] = TEAMNAME2ID.get((soup.find_all('span', class_='hide-mobile')[count].text),(soup.find_all('span', class_='hide-mobile')[count].text))
                data["type_rank"] = "league"
                data["num_rank"] = count - 14
                data["area"] = area[1].text
                data["year_game"] = url.split("/")[7]
                data["win"] = (i.find_all("td")[0].text)
                data["losses"]  = (i.find_all("td")[1].text)
                data["winning_percentage"] = (i.find_all("td")[2].text)
                data["games_back"] = (i.find_all("td")[3].text).replace("-","")
                data["win_home"] = (i.find_all("td")[4].text).split("-")[0]
                data["loss_home"] = (i.find_all("td")[4].text).split("-")[1]
                data["win_away"] = (i.find_all("td")[5].text). split("-")[0]
                data["loss_away"] = (i.find_all("td")[5].text).split("-")[1]
                data["rs"] = (i.find_all("td")[6].text)
                data["ra"] = (i.find_all("td")[7].text)
                data["diff"] = (i.find_all("td")[8].text)
                data["strk"] = (i.find_all("td")[9].text)
                data["l10_win"] = (i.find_all("td")[10].text).split("-")[0]
                data["l10_loss"] = (i.find_all("td")[10].text).split("-")[1]
                td_elements = i.find_all("td")
                if len(td_elements) > 11:
                    data["poff"] = td_elements[11].text
                else:
                    data["poff"] = ""
                with write_lock:
                    df = pd.DataFrame([data])
                    df.to_csv('./data/rank_team_standing.csv', mode='a', header=not pd.io.common.file_exists('./data/rank_team_standing.csv'), index=False)

                count += 1
        except Exception as e:
            print(e)
            pass


    list_url = [f"https://www.espn.com/mlb/standings/_/season/{i}/group/league" for i in range(START_YEAR, END_YEAR+1)]

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(get_data, list_url)
        
        
        
    import pandas as pd
    import requests
    from bs4 import BeautifulSoup
    from concurrent.futures import ThreadPoolExecutor
    from threading import Lock
    from MLB.config.params import START_YEAR, END_YEAR

    write_lock = Lock()
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

    def get_data(url):
        try:
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            tagert = (soup.find_all("tbody", class_="Table__TBODY")[1])
            tagerts = tagert.find_all("tr")
            list_area = ["East", "Central", "West"]
            count = 0
            positions_to_remove = [30, 24, 18, 12, 6, 0]

            for pos in positions_to_remove:
                if len(tagerts) > pos:
                    tagerts.pop(pos)
            flag_area = 0
            flag_id = 0
            for i in tagerts:
                data = dict()
                data["team_id"] = TEAMNAME2ID.get((soup.find_all('span', class_='hide-mobile')[flag_id].text),(soup.find_all('span', class_='hide-mobile')[flag_id].text))
                data["type_rank"] = "division"
                data["num_rank"] =  count + 1
                data["area"] = soup.find_all("div",class_="Table__Title")[0].text + "_" + list_area[flag_area]
                data["year_game"] = url.split("/")[7]
                data["win"] = (i.find_all("td")[0].text)
                data["losses"]  = (i.find_all("td")[1].text)
                data["winning_percentage"] = (i.find_all("td")[2].text)
                data["games_back"] = (i.find_all("td")[3].text).replace("-","")
                data["win_home"] = (i.find_all("td")[4].text).split("-")[0]
                data["loss_home"] = (i.find_all("td")[4].text).split("-")[1]
                data["win_away"] = (i.find_all("td")[5].text). split("-")[0]
                data["loss_away"] = (i.find_all("td")[5].text).split("-")[1]
                data["rs"] = (i.find_all("td")[6].text)
                data["ra"] = (i.find_all("td")[7].text)
                data["diff"] = (i.find_all("td")[8].text)
                data["strk"] = (i.find_all("td")[9].text)
                data["l10_win"] = (i.find_all("td")[10].text).split("-")[0]
                data["l10_loss"] = (i.find_all("td")[10].text).split("-")[1]
                td_elements = i.find_all("td")
                if len(td_elements) > 11:
                    data["poff"] = td_elements[11].text
                else:
                    data["poff"] = ""
                with write_lock:
                    df = pd.DataFrame([data])
                    df.to_csv('./data/rank_team_standing.csv', mode='a', header=not pd.io.common.file_exists('./data/rank_team_standing.csv'), index=False)

                if count > 3 :
                    count = count -5
                    flag_area += 1 
                count += 1
                flag_id += 1



            tagert = (soup.find_all("tbody", class_="Table__TBODY")[3])
            tagerts = tagert.find_all("tr")
            list_area = ["East", "Central", "West"]
            flag_area = 0

            positions_to_remove = [30, 24, 18, 12, 6, 0]

            for pos in positions_to_remove:
                if len(tagerts) > pos:
                    tagerts.pop(pos)
            count = 0
            flag_id = 15
            for i in tagerts:
                data = dict()
                data["team_id"] = TEAMNAME2ID.get((soup.find_all('span', class_='hide-mobile')[flag_id].text),(soup.find_all('span', class_='hide-mobile')[flag_id].text))
                data["type_rank"] = "division"
                data["num_rank"] = count + 1
                data["area"] = soup.find_all("div",class_="Table__Title")[1].text + "_" + list_area[flag_area]
                data["year_game"] = url.split("/")[7]
                data["win"] = (i.find_all("td")[0].text)
                data["losses"]  = (i.find_all("td")[1].text)
                data["winning_percentage"] = (i.find_all("td")[2].text)
                data["games_back"] = (i.find_all("td")[3].text).replace("-","")
                data["win_home"] = (i.find_all("td")[4].text).split("-")[0]
                data["loss_home"] = (i.find_all("td")[4].text).split("-")[1]
                data["win_away"] = (i.find_all("td")[5].text). split("-")[0]
                data["loss_away"] = (i.find_all("td")[5].text).split("-")[1]
                data["rs"] = (i.find_all("td")[6].text)
                data["ra"] = (i.find_all("td")[7].text)
                data["diff"] = (i.find_all("td")[8].text)
                data["strk"] = (i.find_all("td")[9].text)
                data["l10_win"] = (i.find_all("td")[10].text).split("-")[0]
                data["l10_loss"] = (i.find_all("td")[10].text).split("-")[1]
                td_elements = i.find_all("td")
                if len(td_elements) > 11:
                    data["poff"] = td_elements[11].text
                else:
                    data["poff"] = ""
                with write_lock:
                    df = pd.DataFrame([data])
                    df.to_csv('./data/rank_team_standing.csv', mode='a', header=not pd.io.common.file_exists('./data/rank_team_standing.csv'), index=False)

                if count > 3 :
                    count = count -5
                    flag_area += 1 
                count += 1
                flag_id += 1
                # print(flag_area)
                
        except Exception as e:
            print(e)
            pass

    list_url = [f"https://www.espn.com/mlb/standings/_/season/{i}" for i in range(START_YEAR, END_YEAR+1)]

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(get_data, list_url)
        
        
        
        
        
    #________________________________________________________________________________________________________________________WILDCARD________________________________________________________________________________________________________________________
    import pandas as pd
    import requests
    from bs4 import BeautifulSoup
    from concurrent.futures import ThreadPoolExecutor
    from threading import Lock
    from MLB.config.params import START_YEAR, END_YEAR

    write_lock = Lock()
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


    def get_data(url):
        try:
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            tagert = (soup.find_all("tbody", class_="Table__TBODY")[1])
            count = 0
            for i in tagert.find_all("tr"):
                data = dict()
                data["team_id"] = TEAMNAME2ID.get((soup.find_all('span', class_='hide-mobile')[count].text),(soup.find_all('span', class_='hide-mobile')[count].text))
                data["num_rank"] = count + 1 
                data["area"] = soup.find_all('div', class_='Table__Title')[0].text
                data["year_game"] = url.split("/")[7]
                data["win"] = (i.find_all("td")[0].text)
                data["losses"]  = (i.find_all("td")[1].text)
                data["winning_percentage"] = (i.find_all("td")[2].text)
                data["games_back"] = (i.find_all("td")[3].text).replace("-","")
                data["win_home"] = (i.find_all("td")[4].text).split("-")[0]
                data["loss_home"] = (i.find_all("td")[4].text).split("-")[1]
                data["win_away"] = (i.find_all("td")[5].text). split("-")[0]
                data["loss_away"] = (i.find_all("td")[5].text).split("-")[1]
                data["rs"] = (i.find_all("td")[6].text)
                data["ra"] = (i.find_all("td")[7].text)
                data["diff"] = (i.find_all("td")[8].text)
                data["strk"] = (i.find_all("td")[9].text)
                data["l10_win"] = (i.find_all("td")[10].text).split("-")[0]
                data["l10_loss"] = (i.find_all("td")[10].text).split("-")[1]

                with write_lock:
                    df = pd.DataFrame([data])
                    df.to_csv('./data/rank_team_wildcard.csv', mode='a', header=not pd.io.common.file_exists('./data/rank_team_wildcard.csv'), index=False)
                count += 1
                
            tagert = (soup.find_all("tbody", class_="Table__TBODY")[3])
            count = 12
            for i in tagert.find_all("tr"):
                data = dict()
                data["team_id"] = TEAMNAME2ID.get((soup.find_all('span', class_='hide-mobile')[count].text),(soup.find_all('span', class_='hide-mobile')[count].text))
                data["num_rank"] = count - 11
                data["area"] = soup.find_all('div', class_='Table__Title')[1].text
                data["year_game"] = url.split("/")[7]
                data["win"] = (i.find_all("td")[0].text)
                data["losses"]  = (i.find_all("td")[1].text)
                data["winning_percentage"] = (i.find_all("td")[2].text)
                data["games_back"] = (i.find_all("td")[3].text)
                data["win_home"] = (i.find_all("td")[4].text).split("-")[0]
                data["loss_home"] = (i.find_all("td")[4].text).split("-")[1]
                data["win_away"] = (i.find_all("td")[5].text). split("-")[0]
                data["loss_away"] = (i.find_all("td")[5].text).split("-")[1]
                data["rs"] = (i.find_all("td")[6].text)
                data["ra"] = (i.find_all("td")[7].text)
                data["diff"] = (i.find_all("td")[8].text)
                data["strk"] = (i.find_all("td")[9].text)
                data["l10_win"] = (i.find_all("td")[10].text).split("-")[0]
                data["l10_loss"] = (i.find_all("td")[10].text).split("-")[1]

                with write_lock:
                    df = pd.DataFrame([data])
                    df.to_csv('./data/rank_team_wildcard.csv', mode='a', header=not pd.io.common.file_exists('./data/rank_team_wildcard.csv'), index=False)
                count += 1
        except Exception as e:
            print(e)
            pass

    list_url = [f"https://www.espn.com/mlb/standings/_/season/{i}/group/league/view/wild-card" for i in range(START_YEAR, END_YEAR+1)]

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(get_data, list_url)
        
    #________________________________________________________________________________________________________________________EXPANDED________________________________________________________________________________________________________________________

    import pandas as pd
    import requests
    from bs4 import BeautifulSoup
    from concurrent.futures import ThreadPoolExecutor
    from threading import Lock
    from MLB.config.params import START_YEAR, END_YEAR
    write_lock = Lock()
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

        

    def get_data(url):
        try:
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            tagert = (soup.find_all("tbody", class_="Table__TBODY")[1])
            count = 0
            for i in tagert.find_all("tr"):
                data = dict()
                data["team_id"] = TEAMNAME2ID.get((soup.find_all('span', class_='hide-mobile')[count].text),(soup.find_all('span', class_='hide-mobile')[count].text))
                data["type_rank"] = "overall"
                data["num_rank"] = count + 1
                data["area"] = soup.find("div",class_="Table__Title").text
                data["year_game"] = url.split("/")[7]
                data["win"] = (i.find_all("td")[0].text)
                data["losses"]  = (i.find_all("td")[1].text)
                data["winning_percentage"] = (i.find_all("td")[2].text)
                data["games_back"] = (i.find_all("td")[3].text).replace("-","")
                data["win_day"] = (i.find_all("td")[4].text).split("-")[0]
                data["loss_day"] = (i.find_all("td")[4].text).split("-")[1]
                data["win_night"] = (i.find_all("td")[5].text). split("-")[0]
                data["loss_night"] = (i.find_all("td")[5].text).split("-")[1]
                data["win_1run"] = (i.find_all("td")[6].text).split("-")[0]
                data["loss_1run"] = (i.find_all("td")[6].text).split("-")[1]
                data["win_xtra"] = (i.find_all("td")[7].text).split("-")[0]
                data["loss_xtra"] = (i.find_all("td")[7].text).split("-")[1]
                data["win_exwl"] = (i.find_all("td")[8].text).split("-")[0]
                data["loss_exwl"] = (i.find_all("td")[8].text).split("-")[1]
                with write_lock:
                    df = pd.DataFrame([data])
                    df.to_csv('./data/rank_team_expanded.csv', mode='a', header=not pd.io.common.file_exists('./data/rank_team_expanded.csv'), index=False)

                count += 1
        except Exception as e:
            print(e)
            pass

    list_url = [f"https://www.espn.com/mlb/standings/_/season/{i}/group/overall/view/expanded" for i in range(START_YEAR, END_YEAR+1)]

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(get_data, list_url)



    import pandas as pd
    import requests
    from bs4 import BeautifulSoup
    from concurrent.futures import ThreadPoolExecutor
    from threading import Lock

    write_lock = Lock()
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
    def get_data(url):
        try:
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            tagert = (soup.find_all("tbody", class_="Table__TBODY")[1])
            area = soup.find_all("div",class_="Table__Title")
            count = 0
            for i in tagert.find_all("tr"):
                data = dict()
                data["team_id"] = TEAMNAME2ID.get((soup.find_all('span', class_='hide-mobile')[count].text),(soup.find_all('span', class_='hide-mobile')[count].text))
                data["type_rank"] = "league"
                data["num_rank"] = count + 1
                data["area"] = area[0].text
                data["year_game"] = url.split("/")[7]
                data["win"] = (i.find_all("td")[0].text)
                data["losses"]  = (i.find_all("td")[1].text)
                data["winning_percentage"] = (i.find_all("td")[2].text)
                data["games_back"] = (i.find_all("td")[3].text).replace("-","")
                data["win_day"] = (i.find_all("td")[4].text).split("-")[0]
                data["loss_day"] = (i.find_all("td")[4].text).split("-")[1]
                data["win_night"] = (i.find_all("td")[5].text). split("-")[0]
                data["loss_night"] = (i.find_all("td")[5].text).split("-")[1]
                data["win_1run"] = (i.find_all("td")[6].text).split("-")[0]
                data["loss_1run"] = (i.find_all("td")[6].text).split("-")[1]
                data["win_xtra"] = (i.find_all("td")[7].text).split("-")[0]
                data["loss_xtra"] = (i.find_all("td")[7].text).split("-")[1]
                data["win_exwl"] = (i.find_all("td")[8].text).split("-")[0]
                data["loss_exwl"] = (i.find_all("td")[8].text).split("-")[1]
                # td_elements = i.find_all("td")
                # if len(td_elements) > 11:
                #     data["poff"] = td_elements[11].text
                # else:
                #     data["poff"] = ""
                with write_lock:
                    df = pd.DataFrame([data])
                    df.to_csv('./data/rank_team_expanded.csv', mode='a', header=not pd.io.common.file_exists('./data/rank_team_expanded.csv'), index=False)

                count += 1
                
            tagert = (soup.find_all("tbody", class_="Table__TBODY")[3])
            count = 15
            for i in tagert.find_all("tr"):
                data = dict()
                data["team_id"] = TEAMNAME2ID.get((soup.find_all('span', class_='hide-mobile')[count].text),(soup.find_all('span', class_='hide-mobile')[count].text))
                data["type_rank"] = "league"
                data["num_rank"] = count - 14
                data["area"] = area[1].text
                data["year_game"] = url.split("/")[7]
                data["win"] = (i.find_all("td")[0].text)
                data["losses"]  = (i.find_all("td")[1].text)
                data["winning_percentage"] = (i.find_all("td")[2].text)
                data["games_back"] = (i.find_all("td")[3].text).replace("-","")
                data["win_day"] = (i.find_all("td")[4].text).split("-")[0]
                data["loss_day"] = (i.find_all("td")[4].text).split("-")[1]
                data["win_night"] = (i.find_all("td")[5].text). split("-")[0]
                data["loss_night"] = (i.find_all("td")[5].text).split("-")[1]
                data["win_1run"] = (i.find_all("td")[6].text).split("-")[0]
                data["loss_1run"] = (i.find_all("td")[6].text).split("-")[1]
                data["win_xtra"] = (i.find_all("td")[7].text).split("-")[0]
                data["loss_xtra"] = (i.find_all("td")[7].text).split("-")[1]
                data["win_exwl"] = (i.find_all("td")[8].text).split("-")[0]
                data["loss_exwl"] = (i.find_all("td")[8].text).split("-")[1]
                with write_lock:
                    df = pd.DataFrame([data])
                    df.to_csv('./data/rank_team_expanded.csv', mode='a', header=not pd.io.common.file_exists('./data/rank_team_expanded.csv'), index=False)

                count += 1
        except Exception as e:
            print(e)
            pass


    list_url = [f"https://www.espn.com/mlb/standings/_/season/{i}/group/league/view/expanded" for i in range(START_YEAR, END_YEAR+1)]

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(get_data, list_url)
        
        
        
    import pandas as pd
    import requests
    from bs4 import BeautifulSoup
    from concurrent.futures import ThreadPoolExecutor
    from threading import Lock
    from MLB.config.params import START_YEAR, END_YEAR

    write_lock = Lock()
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

    def get_data(url):
        try:
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            tagert = (soup.find_all("tbody", class_="Table__TBODY")[1])
            tagerts = tagert.find_all("tr")
            list_area = ["East", "Central", "West"]
            count = 0
            positions_to_remove = [30, 24, 18, 12, 6, 0]

            for pos in positions_to_remove:
                if len(tagerts) > pos:
                    tagerts.pop(pos)
            flag_area = 0
            flag_id = 0
            for i in tagerts:
                data = dict()
                data["team_id"] = TEAMNAME2ID.get((soup.find_all('span', class_='hide-mobile')[flag_id].text),(soup.find_all('span', class_='hide-mobile')[flag_id].text))
                data["type_rank"] = "division"
                data["num_rank"] =  count + 1
                data["area"] = soup.find_all("div",class_="Table__Title")[0].text + "_" + list_area[flag_area]
                data["year_game"] = url.split("/")[7]
                data["win"] = (i.find_all("td")[0].text)
                data["losses"]  = (i.find_all("td")[1].text)
                data["winning_percentage"] = (i.find_all("td")[2].text)
                data["games_back"] = (i.find_all("td")[3].text).replace("-","")
                data["win_day"] = (i.find_all("td")[4].text).split("-")[0]
                data["loss_day"] = (i.find_all("td")[4].text).split("-")[1]
                data["win_night"] = (i.find_all("td")[5].text). split("-")[0]
                data["loss_night"] = (i.find_all("td")[5].text).split("-")[1]
                data["win_1run"] = (i.find_all("td")[6].text).split("-")[0]
                data["loss_1run"] = (i.find_all("td")[6].text).split("-")[1]
                data["win_xtra"] = (i.find_all("td")[7].text).split("-")[0]
                data["loss_xtra"] = (i.find_all("td")[7].text).split("-")[1]
                data["win_exwl"] = (i.find_all("td")[8].text).split("-")[0]
                data["loss_exwl"] = (i.find_all("td")[8].text).split("-")[1]
                with write_lock:
                    df = pd.DataFrame([data])
                    df.to_csv('./data/rank_team_expanded.csv', mode='a', header=not pd.io.common.file_exists('./data/rank_team_expanded.csv'), index=False)

                if count > 3 :
                    count = count -5
                    flag_area += 1 
                count += 1
                flag_id += 1



            tagert = (soup.find_all("tbody", class_="Table__TBODY")[3])
            tagerts = tagert.find_all("tr")
            list_area = ["East", "Central", "West"]
            flag_area = 0

            positions_to_remove = [30, 24, 18, 12, 6, 0]

            for pos in positions_to_remove:
                if len(tagerts) > pos:
                    tagerts.pop(pos)
            count = 0
            flag_id = 15
            for i in tagerts:
                data = dict()
                data["team_id"] = TEAMNAME2ID.get((soup.find_all('span', class_='hide-mobile')[flag_id].text),(soup.find_all('span', class_='hide-mobile')[flag_id].text))
                data["type_rank"] = "division"
                data["num_rank"] = count + 1
                data["area"] = soup.find_all("div",class_="Table__Title")[1].text + "_" + list_area[flag_area]
                data["year_game"] = url.split("/")[7]
                data["win"] = (i.find_all("td")[0].text)
                data["losses"]  = (i.find_all("td")[1].text)
                data["winning_percentage"] = (i.find_all("td")[2].text)
                data["games_back"] = (i.find_all("td")[3].text).replace("-","")
                data["win_day"] = (i.find_all("td")[4].text).split("-")[0]
                data["loss_day"] = (i.find_all("td")[4].text).split("-")[1]
                data["win_night"] = (i.find_all("td")[5].text). split("-")[0]
                data["loss_night"] = (i.find_all("td")[5].text).split("-")[1]
                data["win_1run"] = (i.find_all("td")[6].text).split("-")[0]
                data["loss_1run"] = (i.find_all("td")[6].text).split("-")[1]
                data["win_xtra"] = (i.find_all("td")[7].text).split("-")[0]
                data["loss_xtra"] = (i.find_all("td")[7].text).split("-")[1]
                data["win_exwl"] = (i.find_all("td")[8].text).split("-")[0]
                data["loss_exwl"] = (i.find_all("td")[8].text).split("-")[1]
                with write_lock:
                    df = pd.DataFrame([data])
                    df.to_csv('./data/rank_team_expanded.csv', mode='a', header=not pd.io.common.file_exists('./data/rank_team_expanded.csv'), index=False)

                if count > 3 :
                    count = count -5
                    flag_area += 1 
                count += 1
                flag_id += 1
                # print(flag_area)
                
        except Exception as e:
            print(e)
            pass

    list_url = [f"https://www.espn.com/mlb/standings/_/season/{i}/view/expanded" for i in range(START_YEAR, END_YEAR+1)]

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(get_data, list_url)
        
    #________________________________________________________________________________________________________________________VSDAVISION________________________________________________________________________________________________________________________


    import pandas as pd
    import requests
    from bs4 import BeautifulSoup
    from concurrent.futures import ThreadPoolExecutor
    from threading import Lock
    from MLB.config.params import START_YEAR, END_YEAR
    write_lock = Lock()
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

        

    def get_data(url):
        try:
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            tagert = (soup.find_all("tbody", class_="Table__TBODY")[1])
            count = 0
            for i in tagert.find_all("tr"):
                data = dict()
                data["team_id"] = TEAMNAME2ID.get((soup.find_all('span', class_='hide-mobile')[count].text),(soup.find_all('span', class_='hide-mobile')[count].text))
                data["type_rank"] = "overall"
                data["num_rank"] = count + 1
                data["area"] = soup.find("div",class_="Table__Title").text
                data["year_game"] = url.split("/")[7]
                data["win"] = (i.find_all("td")[0].text)
                data["losses"]  = (i.find_all("td")[1].text)
                data["winning_percentage"] = (i.find_all("td")[2].text)
                data["games_back"] = (i.find_all("td")[3].text).replace("-", "")
                data["win_east"] = (i.find_all("td")[4].text).split("-")[0]
                data["loss_east"] = (i.find_all("td")[4].text).split("-")[1]
                data["win_cent"] = (i.find_all("td")[5].text). split("-")[0]
                data["loss_cent"] = (i.find_all("td")[5].text).split("-")[1]
                data["win_west"] = (i.find_all("td")[6].text).split("-")[0]
                data["loss_west"] = (i.find_all("td")[6].text).split("-")[1]
                data["win_intr"] = (i.find_all("td")[7].text).split("-")[0]
                data["loss_intr"] = (i.find_all("td")[7].text).split("-")[1]
                data["win_rhp"] = (i.find_all("td")[8].text).split("-")[0]
                data["loss_rhp"] = (i.find_all("td")[8].text).split("-")[1]
                data["win_lhp"] = (i.find_all("td")[9].text).split("-")[0]
                data["loss_lhp"] = (i.find_all("td")[9].text).split("-")[1]
                with write_lock:
                    df = pd.DataFrame([data])
                    df.to_csv('./data/rank_team_vs_division.csv', mode='a', header=not pd.io.common.file_exists('./data/rank_team_vs_division.csv'), index=False)

                count += 1
        except Exception as e:
            print(e)
            pass

    list_url = [f"https://www.espn.com/mlb/standings/_/season/{i}/group/overall/view/vs-division" for i in range(START_YEAR, END_YEAR+1)]


    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(get_data, list_url)



    import pandas as pd
    import requests
    from bs4 import BeautifulSoup
    from concurrent.futures import ThreadPoolExecutor
    from threading import Lock

    write_lock = Lock()
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
    def get_data(url):
        try:
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            tagert = (soup.find_all("tbody", class_="Table__TBODY")[1])
            area = soup.find_all("div",class_="Table__Title")
            count = 0
            for i in tagert.find_all("tr"):
                data = dict()
                data["team_id"] = TEAMNAME2ID.get((soup.find_all('span', class_='hide-mobile')[count].text),(soup.find_all('span', class_='hide-mobile')[count].text))
                data["type_rank"] = "league"
                data["num_rank"] = count + 1
                data["area"] = area[0].text
                data["year_game"] = url.split("/")[7]
                data["win"] = (i.find_all("td")[0].text)
                data["losses"]  = (i.find_all("td")[1].text)
                data["winning_percentage"] = (i.find_all("td")[2].text)
                data["games_back"] = (i.find_all("td")[3].text).replace("-", "")
                data["win_east"] = (i.find_all("td")[4].text).split("-")[0]
                data["loss_east"] = (i.find_all("td")[4].text).split("-")[1]
                data["win_cent"] = (i.find_all("td")[5].text). split("-")[0]
                data["loss_cent"] = (i.find_all("td")[5].text).split("-")[1]
                data["win_west"] = (i.find_all("td")[6].text).split("-")[0]
                data["loss_west"] = (i.find_all("td")[6].text).split("-")[1]
                data["win_intr"] = (i.find_all("td")[7].text).split("-")[0]
                data["loss_intr"] = (i.find_all("td")[7].text).split("-")[1]
                data["win_rhp"] = (i.find_all("td")[8].text).split("-")[0]
                data["loss_rhp"] = (i.find_all("td")[8].text).split("-")[1]
                data["win_lhp"] = (i.find_all("td")[9].text).split("-")[0]
                data["loss_lhp"] = (i.find_all("td")[9].text).split("-")[1]
                # td_elements = i.find_all("td")
                # if len(td_elements) > 11:
                #     data["poff"] = td_elements[11].text
                # else:
                #     data["poff"] = ""
                with write_lock:
                    df = pd.DataFrame([data])
                    df.to_csv('./data/rank_team_vs_division.csv', mode='a', header=not pd.io.common.file_exists('./data/rank_team_vs_division.csv'), index=False)

                count += 1
                
            tagert = (soup.find_all("tbody", class_="Table__TBODY")[3])
            count = 15
            for i in tagert.find_all("tr"):
                data = dict()
                data["team_id"] = TEAMNAME2ID.get((soup.find_all('span', class_='hide-mobile')[count].text),(soup.find_all('span', class_='hide-mobile')[count].text))
                data["type_rank"] = "league"
                data["num_rank"] = count - 14
                data["area"] = area[1].text
                data["year_game"] = url.split("/")[7]
                data["win"] = (i.find_all("td")[0].text)
                data["losses"]  = (i.find_all("td")[1].text)
                data["winning_percentage"] = (i.find_all("td")[2].text)
                data["games_back"] = (i.find_all("td")[3].text).replace("-", "")
                data["win_east"] = (i.find_all("td")[4].text).split("-")[0]
                data["loss_east"] = (i.find_all("td")[4].text).split("-")[1]
                data["win_cent"] = (i.find_all("td")[5].text). split("-")[0]
                data["loss_cent"] = (i.find_all("td")[5].text).split("-")[1]
                data["win_west"] = (i.find_all("td")[6].text).split("-")[0]
                data["loss_west"] = (i.find_all("td")[6].text).split("-")[1]
                data["win_intr"] = (i.find_all("td")[7].text).split("-")[0]
                data["loss_intr"] = (i.find_all("td")[7].text).split("-")[1]
                data["win_rhp"] = (i.find_all("td")[8].text).split("-")[0]
                data["loss_rhp"] = (i.find_all("td")[8].text).split("-")[1]
                data["win_lhp"] = (i.find_all("td")[9].text).split("-")[0]
                data["loss_lhp"] = (i.find_all("td")[9].text).split("-")[1]
                with write_lock:
                    df = pd.DataFrame([data])
                    df.to_csv('./data/rank_team_vs_division.csv', mode='a', header=not pd.io.common.file_exists('./data/rank_team_vs_division.csv'), index=False)

                count += 1
        except Exception as e:
            print(e)
            pass


    list_url = [f"https://www.espn.com/mlb/standings/_/season/{i}/group/league/view/vs-division" for i in range(START_YEAR, END_YEAR+1)]

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(get_data, list_url)
        
        
        
    import pandas as pd
    import requests
    from bs4 import BeautifulSoup
    from concurrent.futures import ThreadPoolExecutor
    from threading import Lock
    from MLB.config.params import START_YEAR, END_YEAR

    write_lock = Lock()
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

    def get_data(url):
        try:
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            tagert = (soup.find_all("tbody", class_="Table__TBODY")[1])
            tagerts = tagert.find_all("tr")
            list_area = ["East", "Central", "West"]
            count = 0
            positions_to_remove = [30, 24, 18, 12, 6, 0]

            for pos in positions_to_remove:
                if len(tagerts) > pos:
                    tagerts.pop(pos)
            flag_area = 0
            flag_id = 0
            for i in tagerts:
                data = dict()
                data["team_id"] = TEAMNAME2ID.get((soup.find_all('span', class_='hide-mobile')[flag_id].text),(soup.find_all('span', class_='hide-mobile')[flag_id].text))
                data["type_rank"] = "division"
                data["num_rank"] =  count + 1
                data["area"] = soup.find_all("div",class_="Table__Title")[0].text + "_" + list_area[flag_area]
                data["year_game"] = url.split("/")[7]
                data["win"] = (i.find_all("td")[0].text)
                data["losses"]  = (i.find_all("td")[1].text)
                data["winning_percentage"] = (i.find_all("td")[2].text)
                data["games_back"] = (i.find_all("td")[3].text).replace("-", "")
                data["win_east"] = (i.find_all("td")[4].text).split("-")[0]
                data["loss_east"] = (i.find_all("td")[4].text).split("-")[1]
                data["win_cent"] = (i.find_all("td")[5].text). split("-")[0]
                data["loss_cent"] = (i.find_all("td")[5].text).split("-")[1]
                data["win_west"] = (i.find_all("td")[6].text).split("-")[0]
                data["loss_west"] = (i.find_all("td")[6].text).split("-")[1]
                data["win_intr"] = (i.find_all("td")[7].text).split("-")[0]
                data["loss_intr"] = (i.find_all("td")[7].text).split("-")[1]
                data["win_rhp"] = (i.find_all("td")[8].text).split("-")[0]
                data["loss_rhp"] = (i.find_all("td")[8].text).split("-")[1]
                data["win_lhp"] = (i.find_all("td")[9].text).split("-")[0]
                data["loss_lhp"] = (i.find_all("td")[9].text).split("-")[1]
                with write_lock:
                    df = pd.DataFrame([data])
                    df.to_csv('./data/rank_team_vs_division.csv', mode='a', header=not pd.io.common.file_exists('./data/rank_team_vs_division.csv'), index=False)

                if count > 3 :
                    count = count -5
                    flag_area += 1 
                count += 1
                flag_id += 1



            tagert = (soup.find_all("tbody", class_="Table__TBODY")[3])
            tagerts = tagert.find_all("tr")
            list_area = ["East", "Central", "West"]
            flag_area = 0

            positions_to_remove = [30, 24, 18, 12, 6, 0]

            for pos in positions_to_remove:
                if len(tagerts) > pos:
                    tagerts.pop(pos)
            count = 0
            flag_id = 15
            for i in tagerts:
                data = dict()
                data["team_id"] = TEAMNAME2ID.get((soup.find_all('span', class_='hide-mobile')[flag_id].text),(soup.find_all('span', class_='hide-mobile')[flag_id].text))
                data["type_rank"] = "division"
                data["num_rank"] = count + 1
                data["area"] = soup.find_all("div",class_="Table__Title")[1].text + "_" + list_area[flag_area]
                data["year_game"] = url.split("/")[7]
                data["win"] = (i.find_all("td")[0].text)
                data["losses"]  = (i.find_all("td")[1].text)
                data["winning_percentage"] = (i.find_all("td")[2].text)
                data["games_back"] = (i.find_all("td")[3].text).replace("-", "")
                data["win_east"] = (i.find_all("td")[4].text).split("-")[0]
                data["loss_east"] = (i.find_all("td")[4].text).split("-")[1]
                data["win_cent"] = (i.find_all("td")[5].text). split("-")[0]
                data["loss_cent"] = (i.find_all("td")[5].text).split("-")[1]
                data["win_west"] = (i.find_all("td")[6].text).split("-")[0]
                data["loss_west"] = (i.find_all("td")[6].text).split("-")[1]
                data["win_intr"] = (i.find_all("td")[7].text).split("-")[0]
                data["loss_intr"] = (i.find_all("td")[7].text).split("-")[1]
                data["win_rhp"] = (i.find_all("td")[8].text).split("-")[0]
                data["loss_rhp"] = (i.find_all("td")[8].text).split("-")[1]
                data["win_lhp"] = (i.find_all("td")[9].text).split("-")[0]
                data["loss_lhp"] = (i.find_all("td")[9].text).split("-")[1]
                with write_lock:
                    df = pd.DataFrame([data])
                    df.to_csv('./data/rank_team_vs_division.csv', mode='a', header=not pd.io.common.file_exists('./data/rank_team_vs_division.csv'), index=False)

                if count > 3 :
                    count = count -5
                    flag_area += 1 
                count += 1
                flag_id += 1
                # print(flag_area)
                
        except Exception as e:
            print(e)
            pass

    list_url = [f"https://www.espn.com/mlb/standings/_/season/{i}/view/vs-division" for i in range(START_YEAR, END_YEAR+1)]

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(get_data, list_url)
        
        
        
    #________________________________________________________________________________________________________________________END________________________________________________________________________________________________________________________