import json

import pandas as pd
import requests

from dagster import AssetExecutionContext, MetadataValue, asset

import psycopg
from psycopg import sql
from psycopg import DatabaseError
import credentials
import rundate
import datetime as dt

@asset
def get_carbon_api_data():
    """Get carbon intensity data from National grid website.

    API Docs: https://carbon-intensity.github.io/api-definitions/?python#carbon-intensity-api-v2-0-0
    """
    time_period_from = rundate.time_period_from
    time_period_to = rundate.time_period_to

    headers = {
        'Accept': 'application/json'
    }
    carbon_api_data = requests.get(
        'https://api.carbonintensity.org.uk/intensity/' + time_period_from +'/' + time_period_to,
        params={},
        headers= headers
    ).json()
    
    carbon_api_data = carbon_api_data['data']
    df = pd.json_normalize(carbon_api_data)
    return (df)

@asset
def get_generation_api_data():
    """Get carbon intensity data from National grid website.

    API Docs: https://carbon-intensity.github.io/api-definitions/?python#carbon-intensity-api-v2-0-0
    """
    time_period_from = rundate.time_period_from
    time_period_to = rundate.time_period_to

    headers = {
        'Accept': 'application/json'
    }
    generation_api_data = requests.get(
        'https://api.carbonintensity.org.uk/generation/' + time_period_from +'/' + time_period_to,
        params={},
        headers= headers
    ).json()
    
    generation_api_data = generation_api_data['data']
    df = pd.json_normalize(generation_api_data,'generationmix',['from','to'])
    return (df)

@asset
def transform_carbon_data(get_carbon_api_data):
    df = get_carbon_api_data
    df['ts'] = pd.to_datetime(df['from'])
    df['ts'] = df['ts'].map(lambda t: t.strftime('%Y-%m-%d %H:%M'))
    df = df[['ts','intensity.index','intensity.actual','intensity.forecast']]
    return(df)

@asset
def transform_generation_data(get_generation_api_data):
    df = get_generation_api_data
    df = df.pivot_table(columns = 'fuel',values = 'perc', index = ['from','to'])
    df.reset_index(inplace = True)
    df['ts'] = pd.to_datetime(df['from'])
    df['ts'] = df['ts'].map(lambda t: t.strftime('%Y-%m-%d %H:%M'))
    df = df[['ts','biomass','coal','gas','hydro','imports','nuclear','other','solar','wind']]
    return(df)


@asset
def load_to_carbon_intensity_table(transform_carbon_data):
    df = transform_carbon_data
    conn = psycopg.connect(
        dbname = "postgres",
        user = credentials.user,
        password = credentials.password,
        port = "5432")
    cur = conn.cursor()
    tuples = list([tuple(x) for x in df.to_numpy()])
    cols = ('ts','indicator','actual','forecast')
    cols = ",".join(cols)
    table = 'carbon_intensity'
    query = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s)" % (table,cols)
    
    try:
        cur.executemany(query,tuples)
        conn.commit()
        
    except (Exception, psycopg.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            return 1
    cur.close()
    conn.close()

@asset
def load_to_generation_mix_table(transform_generation_data):
    df = transform_generation_data
    conn = psycopg.connect(
        dbname = "postgres",
        user = credentials.user,
        password = credentials.password,
        port = "5432")
    cur = conn.cursor()
    tuples = list([tuple(x) for x in df.to_numpy()])
    cols = ('ts','biomass','coal','gas','hydro','imports','nuclear','other','solar','wind')
    cols = ",".join(cols)
    table = 'generation_mix'
    query = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % (table,cols)
    
    try:
        cur.executemany(query,tuples)
        conn.commit()
        
    except (Exception, psycopg.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            return 1
    cur.close()
    conn.close()


@asset
def insert_into_fact_table(load_to_carbon_intensity_table,load_to_generation_mix_table):
    start_time = pd.to_datetime(rundate.time_period_from) - dt.timedelta(hours=0, minutes=30)
    start_time = start_time.strftime('%Y-%m-%d %H:%M')
    end_time = pd.to_datetime(rundate.time_period_to) - dt.timedelta(hours=0, minutes=30)
    end_time = end_time.strftime('%Y-%m-%d %H:%M')

    conn = psycopg.connect(
        dbname = "postgres",
        user = credentials.user,
        password = credentials.password,
        port = "5432")
    cur = conn.cursor()
    
    try:
        cur.execute(
            """
            insert into fact_reporting
            select
                gm.ts,
                date_part('DAY',gm.ts) as ts_day,
                date_part('MONTH',gm.ts) as ts_month,
                date_part('YEAR',gm.ts) as ts_year,
                gm.biomass,
                gm.coal,
                gm.gas,
                gm.hydro,
                gm.imports,
                gm.nuclear,
                gm.other,
                gm.solar,
                gm.wind,
                ci."indicator" ,
                ci.actual,
                ci.forecast 
            from generation_mix gm
            inner join carbon_intensity ci
            on gm.ts = ci.ts
            where gm.ts >= %s
            and gm.ts <= %s
            """,
            (start_time,end_time)
        )
        conn.commit()
        
    except (Exception, psycopg.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            return 1
    cur.close()
    conn.close()

