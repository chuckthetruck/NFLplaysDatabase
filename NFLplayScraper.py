# -*- coding: utf-8 -*-
"""
Created on Wed Sep  2 15:22:48 2020

@author: CJ
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Jun 29 15:17:40 2020

@author: CJ
"""


from bs4 import BeautifulSoup, Comment
from sqlalchemy import create_engine
import re
import requests
import pandas as pd
import mysql.connector

def get_yards(detail):
    
    found = re.search("(-?\d+)\syard",detail)
    
    if found:
        return int(found.group(1))
    
    return 0

def table_to_df(html_table,column_headers):
    records = []
    for idy, row in enumerate(html_table.findAll('tr')):
        if idy == 0:
            continue
        cols = row.findAll(['td','th'])
        record = {}
        for idx, col in enumerate(cols):
            record[column_headers[idx]] = col.text.strip()
        records.append(record)
    
    return pd.DataFrame.from_dict(records)



team_abrv = {	'Arizona Cardinals':['crd','cardinals','ARI'],
        		'Atlanta Falcons':['atl','falcons','ATL'],
        		'Baltimore Ravens':['rav','ravens','BAL'],
        		'Buffalo Bills':['buf','bills','BUF'],
        		'Carolina Panthers':['car','panthers','CAR'],
        		'Chicago Bears':['chi','bears','CHI'],
        		'Cincinnati Bengals':['cin','bengals','CIN'],
        		'Cleveland Browns':['cle','browns','CLE'],
        		'Dallas Cowboys':['dal','cowboys','DAL'],
        		'Denver Broncos':['den','broncos','DEN'],
        		'Detroit Lions':['det','lions','DET'],
        		'Green Bay Packers':['gnb','packers','GNB'],
        		'Houston Texans':['htx','texans','HOU'],
        		'Indianapolis Colts':['clt','colts','IND'],
        		'Jacksonville Jaguars':['jax','jaguars','JAX'],
        		'Kansas City Chiefs':['kan','chiefs','KAN'],
        		'Los Angeles Chargers':['sdg','chargers','LAC'],
        		'Los Angeles Rams':['ram','rams','LAR'],
        		'Miami Dolphins':['mia','miami','MIA'],
        		'Minnesota Vikings':['min','vikings','MIN'],
        		'New England Patriots':['nwe','patriots','NWE'],
        		'New Orleans Saints':['nor','saints','NOR'],
        		'New York Giants':['nyg','giants','NYG'],
        		'New York Jets':['nyj','jets','NYJ'],
        		'Las Vegas Raiders':['rai','raiders','OAK'],
        		'Philadelphia Eagles':['phi','eagles','PHI'],
        		'Pittsburgh Steelers':['pit','steelers','PIT'],
        		'San Francisco 49ers':['sfo','49ers','SFO'],
        		'Seattle Seahawks':['sea','seahawks','SEA'],
        		'Tampa Bay Buccaneers':['tam','buccaneers','TAM'],
        		'Tennessee Titans':['oti','titans','TEN'],
        		'Washington Redskins':['was','redskins','WAS']}

# while(True):
#     team_to_scrape = input('Please select an NFL team in the form [Place] [Nickname]\n')
    
#     if team_to_scrape in team_abrv.keys():
#         break
    
#     print('Not a valid team name.')

engine = create_engine("mysql+mysqlconnector://{user}:{pw}@{srv}"
                           .format(user="testuser",
                                   pw="",
                                   srv="testdb-1.cymwzhs4gg53.us-east-1.rds.amazonaws.com"))
        

existing  = engine.execute("show databases;")
    
existing = [d[0] for d in existing]  
    
if 'NFL2019' not in existing:
    engine.execute("CREATE DATABASE {0}".format('NFL2019'))
    print("Created database {0}".format('NFL2019'))
          
engine = create_engine("mysql+mysqlconnector://{user}:{pw}@{srv}/{db}"
                       .format(user="testuser",
                               pw="",
                               srv="testdb-1.cymwzhs4gg53.us-east-1.rds.amazonaws.com",
                               db='NFL2019'))

for team_to_scrape in team_abrv.keys():
    
    
    html = requests.get('https://www.pro-football-reference.com/teams/'+team_abrv[team_to_scrape][0] +'/2019.htm')
    text = html.text
    soup = BeautifulSoup(text,'html.parser')
    
    table = soup.find('table',{'id':'games'})
    
    boxscore_tags = table.find_all('td',{'data-stat':'boxscore_word'})
    
    boxscore_links = []
    
    for tag in boxscore_tags:
        link = tag.find('a')
        
        if link:
            boxscore_links.append('https://www.pro-football-reference.com' + link['href'])
          
    for link in boxscore_links:      
        print(link)
        html = requests.get(link)
        text = html.text
        soup = BeautifulSoup(text,'html.parser')
        
        for comments in soup.find_all(text= lambda text: isinstance(text, Comment)):
            if "<caption>Full Play-By-Play Table</caption>" in str(comments.extract()):
                pbp_temp = comments.extract()
                
            elif "id=\"home_drives\"" in str(comments.extract()):
                hd_temp = comments.extract()
                
            elif "id=\"vis_drives\"" in str(comments.extract()):
                vd_temp = comments.extract()
        
        html_pbp_table = BeautifulSoup(pbp_temp,'html.parser').find('table') 
        pbp_headers = html_pbp_table.find('thead').text.strip().split('\n')
        
        home_drives_table = BeautifulSoup(hd_temp,'html.parser').find('table') 
        hd_headers = home_drives_table.find('thead').text.strip().split('\n')
        
        vis_drives_table = BeautifulSoup(vd_temp,'html.parser').find('table') 
        hd_headers = vis_drives_table.find('thead').text.strip().split('\n')
        

        pbp_df = table_to_df(html_pbp_table,pbp_headers)
        
        pbp_df = pbp_df.dropna()
        
        cols = list(pbp_df.columns)
        
        pbp_df = pbp_df[pbp_df[team_abrv[team_to_scrape][2]] != '']
        pbp_df = pbp_df[pbp_df.Quarter != 'Quarter']
                
            
        game = cols[6].lower()+'at'+cols[7].lower()+'_'+link.split('/')[-1].split('.')[0][0:-4]
        print(game)
        
        pbp_df = pbp_df[cols]
        
        pbp_df.replace('', 0, inplace = True)
        
        
        
        pbp_df['Time'] = pbp_df['Time'].apply(lambda x: int(x.split(':')[0])*60 + int(x.split(':')[1]))
        
        for i in range(len(pbp_df.columns)):
            if i in [1,2,3,6,7,13]:
                pbp_df[pbp_df.columns[i]] = pbp_df[pbp_df.columns[i]].astype(int)
            
            elif i in [8,9]:
                pbp_df[pbp_df.columns[i]] = pbp_df[pbp_df.columns[i]].astype(float)
        
        pbp_df['Detail'].replace(0,'No Detail', inplace = True)
        
        pbp_df['Type'] = 0
        pbp_df['Depth'] = 0
        pbp_df['Direction']= 0
        pbp_df['Yards Gained']  = 0
        pbp_df.loc[pbp_df['Detail'].str.contains(' pass '),'Type'] = 'pass'
        pbp_df.loc[pbp_df['Detail'].str.contains(' sacked '),'Type'] = 'pass'
        pbp_df.loc[pbp_df['Detail'].str.contains(' spiked '),'Type'] = 'pass'
        pbp_df.loc[pbp_df['Detail'].str.contains(' kicks '),'Type'] = 'special'
        pbp_df.loc[pbp_df['Detail'].str.contains(' punts '),'Type'] = 'special'
        pbp_df.loc[pbp_df['Detail'].str.contains(' field goal '),'Type'] = 'special'
        pbp_df.loc[pbp_df['Detail'].str.contains('no play'),'Type'] = 'no play'
        pbp_df.loc[pbp_df['Type'] == 0,'Type'] = 'rush'
        
        pbp_df.loc[pbp_df['Detail'].str.contains('left'),'Direction'] = 'left'
        pbp_df.loc[pbp_df['Detail'].str.contains('middle'),'Direction'] = 'middle'
        pbp_df.loc[pbp_df['Detail'].str.contains('right'),'Direction'] = 'right'
        pbp_df.loc[pbp_df['Detail'].str.contains(' spiked '),'Direction'] = 'spiked'
        pbp_df.loc[pbp_df['Type'] == 'special','Direction'] = 'special'
        pbp_df.loc[pbp_df['Type'] == 'no play','Direction'] = 'no play'
        pbp_df.loc[pbp_df['Detail'].str.contains(' sacked '),'Direction'] = 'sacked'
        
        pbp_df.loc[pbp_df['Detail'].str.contains(' short '),'Depth'] = 'short'
        pbp_df.loc[pbp_df['Detail'].str.contains(' deep '),'Depth'] = 'deep'
        pbp_df.loc[pbp_df['Detail'].str.contains(' spiked '),'Depth'] = 'spiked'
        pbp_df.loc[pbp_df['Type'] == 'special','Depth'] = 'special'
        pbp_df.loc[pbp_df['Type'] == 'no play','Depth'] = 'no play'
        pbp_df.loc[pbp_df['Type'] == 'rush','Depth'] = 'rush'
        pbp_df.loc[pbp_df['Detail'].str.contains(' sacked '),'Depth'] = 'sacked'
        
        pbp_df['Yards Gained'] = pbp_df['Detail'].apply(get_yards)
        
        
        pbp_df.to_sql(game, con = engine, 
                  if_exists = 'replace', chunksize = 1000)
    
    
    
