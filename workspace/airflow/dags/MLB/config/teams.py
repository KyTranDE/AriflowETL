ALL_TEAM = [
    'Arizona Diamondbacks', 'Atlanta Braves', 'Baltimore Orioles',
    'Boston Red Sox', 'Chicago Cubs', 'Chicago White Sox',
    'Cincinnati Reds', 'Cleveland Guardians', 'Colorado Rockies',
    'Detroit Tigers', 'Houston Astros', 'Kansas City Royals',
    'Los Angeles Angels', 'Los Angeles Dodgers', 'Miami Marlins',
    'Milwaukee Brewers', 'Minnesota Twins', 'New York Mets',
    'New York Yankees', 'Oakland Athletics', 'Philadelphia Phillies',
    'Pittsburgh Pirates', 'San Diego Padres', 'San Francisco Giants',
    'Seattle Mariners', 'St. Louis Cardinals', 'Tampa Bay Rays',
    'Texas Rangers', 'Toronto Blue Jays', 'Washington Nationals'
]

# URL: https://www.baseball-almanac.com/teammenu.shtml
HISTORY_TEAM = {
    'Milwaukee Brewers': 'Baltimore Orioles', # 1901 - 1901
    'St. Louis Browns': 'Baltimore Orioles', # 1902 - 1953
    'Baltimore Orioles': 'Baltimore Orioles', # 1954 - 2023
    
    'Arizona Diamondbacks': 'Arizona Diamondbacks', # 1998 - 2023
    
    'Boston Americans': 'Boston Red Sox', # 1901 - 1907
    'Boston Red Sox': 'Boston Red Sox', # 1908 - 2023
    
    'Boston Red Caps': 'Atlanta Braves', # 1876 - 1882
    'Boston Beaneaters': 'Atlanta Braves', # 1883 - 1906
    'Boston Doves': 'Atlanta Braves', # 1907 - 1910
    'Boston Rustlers': 'Atlanta Braves', # 1911 - 1911
    'Boston Braves': 'Atlanta Braves', # 1912 - 1935 and 1941 - 1952
    'Boston Bees': 'Atlanta Braves', # 1936 - 1940
    'Milwaukee Braves': 'Atlanta Braves', # 1953 - 1965
    'Atlanta Braves': 'Atlanta Braves', # 1966 - 2023
    
    'Chicago White Sox': 'Chicago White Sox', # 1901 - 2023
    
    'Chicago White Stockings': 'Chicago Cubs', # 1876 - 1889
    'Chicago Colts': 'Chicago Cubs', # 1890 - 1897
    'Chicago Orphans': 'Chicago Cubs', # 1898 - 1901
    'Chicago Cubs': 'Chicago Cubs', # 1902 - 2023
    
    'Cleveland Blues': 'Cleveland Guardians', # 1901 - 1904
    'Cleveland Naps': 'Cleveland Guardians', # 1905 - 1914
    'Cleveland Indians': 'Cleveland Guardians', # 1915 - 2021
    'Cleveland Guardians': 'Cleveland Guardians', # 2022 - 2023
    
    'Cincinnati Red Stockings': 'Cincinnati Reds', # 1882 - 1889
    'Cincinnati Reds': 'Cincinnati Reds', # 1890 - 1952
    'Cincinnati Redlegs': 'Cincinnati Reds', # 1953 - 1958
    'Cincinnati Reds': 'Cincinnati Reds', # 1959 - 2023
    
    'Detroit Tigers': 'Detroit Tigers', # 1901 - 2023
    
    'Colorado Rockies': 'Colorado Rockies', # 1993 - 2023
    
    'Houston Colt .45s': 'Houston Astros', # 1962 - 1964
    'Houston Astros': 'Houston Astros', # 1965 - 2023
    
    'Brooklyn Trolley Dodgers': 'Los Angeles Dodgers', # 1884 - 1888
    'Brooklyn Bridegrooms': 'Los Angeles Dodgers', # 1889 - 1898
    'Brooklyn Superbas': 'Los Angeles Dodgers', # 1899 - 1910
    'Brooklyn Dodgers': 'Los Angeles Dodgers', # 1911 - 1931 and 1933 - 1957
    'Brooklyn Robins': 'Los Angeles Dodgers', # 1914 - 1931
    'Los Angeles Dodgers': 'Los Angeles Dodgers', # 1911 - 1931 and 1932 - 2023
    
    'Kansas City Royals': 'Kansas City Royals', # 1969 - 2023
    
    'Florida Marlins': 'Miami Marlins', # 1993 - 2011
    'Miami Marlins': 'Miami Marlins', # 2012 - 2023
    
    'Los Angeles Angels': 'Los Angeles Angels', # 1961 - 1965
    'California Angels': 'Los Angeles Angels', # 1966 - 1996
    'Anaheim Angels': 'Los Angeles Angels', # 1997 - 2004
    'Los Angeles Angels': 'Los Angeles Angels', # 2005 - 2023
    
    'Seattle Pilots': 'Milwaukee Brewers', # 1969 - 1969
    'Milwaukee Brewers': 'Milwaukee Brewers', # 1970 - 2023
    
    'Washington Senators': 'Minnesota Twins', # 1901 - 1960
    'Minnesota Twins': 'Minnesota Twins', # 1961 - 2023
    
    'New York Mets': 'New York Mets', # 1962 - 2023
    
    'Balitmore Orioles': 'New York Yankees', # 1901 - 1902
    'New York Highlanders': 'New York Yankees', # 1903 - 1912
    'New York Yankees': 'New York Yankees', # 1913 - 2023
    
    'Philadelphia Phillies': 'Philadelphia Phillies', # 1883 - 2023
    
    'Philadelphia Athletics': 'Oakland Athletics', # 1901 - 1954
    'Kansas City Athletics': 'Oakland Athletics', # 1955 - 1967
    'Oakland Athletics': 'Oakland Athletics', # 1968 - 2023
    
    'Pittsburgh Alleghenys': 'Pittsburgh Pirates', # 1882 - 1890
    'Pittsburgh Pirates': 'Pittsburgh Pirates', # 1891 - 2023
    
    'Seattle Mariners': 'Seattle Mariners', # 1977 - 2023
    
    'St. Louis Brown Stockings': 'St. Louis Cardinals', # 1882 - 1882
    'St. Louis Browns': 'St. Louis Cardinals', # 1883 - 1898
    'St. Louis Perfectos': 'St. Louis Cardinals', # 1899 - 1899
    'St. Louis Cardinals': 'St. Louis Cardinals', # 1900 - 2023
    
    'Tampa Bay Devil Rays': 'Tampa Bay Rays', # 1998 - 2007
    'Tampa Bay Rays': 'Tampa Bay Rays', # 2008 - 2023
    
    'San Diego Padres': 'San Diego Padres', # 1969 - 2023
    
    'Washington Senators': 'Texas Rangers', # 1961 - 1971
    'Texas Rangers': 'Texas Rangers', # 1972 - 2023
    
    'New York Gothams': 'San Francisco Giants', # 1883 - 1884
    'New York Giants': 'San Francisco Giants', # 1885 - 1957
    'San Francisco Giants': 'San Francisco Giants', # 1958 - 2023
    
    'Toronto Blue Jays': 'Toronto Blue Jays', # 1977 - 2023
    
    'Montreal Expos': 'Washington Nationals', # 1969 - 2004
    'Washington Nationals': 'Washington Nationals', # 2005 - 2023
}
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
    'Cleveland Guardians': 114
}

ID2TEAMNAME = {k:v for v,k in TEAMNAME2ID.items()}

TEAMNAME2CODE = {
    'Atlanta Braves': 'ATL',
    'Texas Rangers': 'TEX',
    'Seattle Mariners': 'SEA',
    'San Francisco Giants': 'SF',
    'Los Angeles Dodgers': 'LAD',
    'Los Angeles Angels': 'LAA',
    'Cincinnati Reds': 'CIN',
    'Arizona Diamondbacks': 'AZ',
    'Philadelphia Phillies': 'PHI',
    'Baltimore Orioles': 'BAL',
    'Milwaukee Brewers': 'MIL',
    'Tampa Bay Rays': 'TB',
    'San Diego Padres': 'SD',
    'Miami Marlins': 'MIA',
    'Boston Red Sox': 'BOS',
    'Kansas City Royals': 'KC',
    'Detroit Tigers': 'DET',
    'New York Mets': 'NYM',
    'Colorado Rockies': 'COL',
    'Pittsburgh Pirates': 'PIT',
    'Toronto Blue Jays': 'TOR',
    'St. Louis Cardinals': 'STL',
    'Chicago White Sox': 'CWS',
    'Washington Nationals': 'WSH',
    'Houston Astros': 'HOU',
    'Chicago Cubs': 'CHC',
    'Minnesota Twins': 'MIN',
    'New York Yankees': 'NYY',
    'Oakland Athletics': 'OAK',
    'Cleveland Guardians': 'CLE'
}

CODE2TEAMNAME = {k:v for v,k in TEAMNAME2CODE.items()}

CODE2ID = {v:TEAMNAME2ID[k] for k,v in TEAMNAME2CODE.items()}

CODE2ID_BETRATE = {
    "BAL": 110,
    "TOR": 141,
    "BOS": 111,
    "COL": 115,
    "NYY": 147,
    "NYM": 121,
    "MIN": 142,
    "MIL": 158,
    "SF": 137,
    "STL": 138,
    "LAA": 108,
    "TEX": 140,
    "PIT": 134,
    "CHC": 112,
    "WSH": 120,
    "HOU": 117,
    "CIN": 113,
    "KC": 118,
    "TB": 139,
    "OAK": 133,
    "MIA": 146,
    "SEA": 136,
    "CLE": 114,
    "SD": 135,
    "PHI": 143,
    "ARI": 109,
    "CHW": 145,
    "LAD": 119,
    "DET": 116,
    "ATL": 144,
    "AL": 159,
    "NL": 160,
}