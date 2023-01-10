####################################################################################
# AUTHOR: Nibir Ray 
# EMAIL: nivirrayy@gmail.com
#
# This file is simple script that is used to inject data from a csv file to a 
# Postgresql database. This is part of a project that is being devloped to 
# be a free football api
#
# PROJECT-LINK: https://github.com/nibirrayy/ft-stats.git 
####################################################################################

import psycopg2
import pandas as pd
from datetime import datetime
import yaml
from yaml.loader import SafeLoader


# Grabbing Postgresql creds from ./../config.yml
with open('./../config.yml') as config:
    data = yaml.load(config, Loader=SafeLoader)

hostname = data['database']['hostname']
database = data['database']['data']
username = data['database']['db']
pwd = data['database']['pwd']
port_id = data['database']['port_id']

# Just to hande things in the finally block
conn = None
cur = None

df = pd.read_csv('./E0.csv')
trimed = df.iloc[:,:24]
shape = trimed.shape

# TODO:change range to range(shape[0]) to go through entire csv
for x in range(shape[0]):
    tmp = trimed.iloc[x]
    tmp = tmp.to_dict()
    
    # This is to correct all format of the dictionary keys value pair
    for key in tmp:
        if key=="Date":
            tmp[key]=datetime.strptime(tmp[key],'%d/%m/%Y').date()
        elif key=="Time":
            tmp[key]=datetime.strptime(tmp[key],'%H:%M').time()


    try:
        conn = psycopg2.connect(
                host = hostname,
                dbname = database,
                user = username,
                password = pwd,
                port = port_id
        )

        cur = conn.cursor()


        fmt_string =  '''
                            INSERT INTO PL_21_22
                            (
                                div,
                                match_date,
                                kickoff,
                                home_team,
                                away_team,
                                FT_home_team_goals,
                                FT_away_team_goals,
                                FT_result,
                                HT_home_team_goals,
                                HT_away_team_goals,
                                HT_result,
                                referee,
                                home_team_shots,
                                away_team_shots,
                                home_team_shots_on_target,
                                away_team_shots_on_target,
                                home_team_fouls_commited,
                                away_team_fouls_commited,
                                home_team_corners,
                                away_team_corners,
                                home_team_yellow_cards,
                                away_team_yellow_cards,
                                home_team_red_cards,
                                away_team_red_cards
                                
                            )
                            VALUES(
                                '{}',
                                (DATE'{}'),
                                (TIME'{}'),
                                '{}',
                                '{}',
                                {},
                                {},
                                '{}',
                                {},
                                {},
                                '{}',
                                '{}',
                                {},
                                {},
                                {},
                                {},
                                {},
                                {},
                                {},
                                {},
                                {},
                                {},
                                {},
                                {}

                            )
                        '''.format(
                            tmp['Div'],
                            tmp['Date'],
                            tmp['Time'],
                            tmp['HomeTeam'],
                            tmp['AwayTeam'],
                            tmp['FTHG'],
                            tmp['FTAG'],
                            tmp['FTR'],
                            tmp['HTHG'],
                            tmp['HTAG'],
                            tmp['HTR'],
                            tmp['Referee'],
                            tmp['HS'],
                            tmp['AS'],
                            tmp['HST'],
                            tmp['AST'],
                            tmp['HF'],
                            tmp['AF'],
                            tmp['HC'],
                            tmp['AC'],
                            tmp['HY'],
                            tmp['AY'],
                            tmp['HR'],
                            tmp['AR'],
                            )
        # print(fmt_string)

        table_insert = fmt_string

        cur.execute(table_insert)

        conn.commit()

    except Exception as error:
        print(error)

    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
