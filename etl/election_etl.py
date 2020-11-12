import time
import logging
import re
import json
from urllib.request import urlopen
from datetime import datetime

from pymongo import MongoClient
from sqlalchemy import create_engine

from spacy.lang.en import English
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from shapely.geometry import Point, Polygon

from postgres_credentials import *
from mongodb_credentials import *

# initialise NLP tools
SENT_ANALYSIS = SentimentIntensityAnalyzer()
NLP = English()

# set up data for USA outline & states
with urlopen('https://raw.githubusercontent.com/johan/world.geo.json/master/countries/USA.geo.json') as usa_url:
    USA_TOTAL = json.load(usa_url)

with urlopen('https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json') as states_url:
    USA_STATES = json.load(states_url)

def make_polygon_list(state):
    '''
    Returns list object containing polygon(s) that make up the state
    state is feature level of USA geojson
    '''
    if state['geometry']['type'] == 'Polygon':
        list_of_polygons = [Polygon(state['geometry']['coordinates'][0])]
    else:
        list_of_polygons = [Polygon(x[0]) for x in state['geometry']['coordinates']]
    return list_of_polygons

LIST_STATES = [state['properties']['name'] for state in USA_STATES['features']]

ALL_POLYGON_LISTS = [make_polygon_list(state) for state in USA_STATES['features']]

STATE_DICT = dict(zip(LIST_STATES, ALL_POLYGON_LISTS))

USA_POLY_LIST = make_polygon_list(USA_TOTAL['features'][0])
# This geojson is missing some large chunks of Alaska and all of Puerto Rico.
# steps:
# 1. remove existing Alaska shape
# 2. re-add the more detailed Alaska and Puerto Rico shapes manually
# (taken from PublicaMundi source)
USA_POLY_LIST = USA_POLY_LIST[:-1]
TO_APPEND = ['Alaska', 'Puerto Rico']
for state in TO_APPEND:
    for poly in STATE_DICT[state]:
        USA_POLY_LIST.append(poly)

USA_DICT = {'United States of America' : USA_POLY_LIST}

def is_point_in_state(point, list_of_polygons):
    '''
    Finds out whether point is within any of the polygons that make up a state
    point: shapely Point
    list_of_polygons: list of the polygons making up each state
    returns boolean
    '''
    list_bool = [point.within(poly) for poly in list_of_polygons]
    return any(list_bool)

def get_state(tweet, search_space):
    '''
    Finds out which boundary a point is within
    When search_space is state_dict, returns state
    When search_space is usa_dict, returns whether point is in USA or not
    '''
    if tweet['location'] not in ['no_loc', 'none']:
        point = Point([float(tweet['loc_lon']), float(tweet['loc_lat'])])
        for place in search_space.items():
            if is_point_in_state(point, place[1]):
                state_name = place[0]
                break
            else:
                state_name = 'other'
    else:
        state_name = 'no_loc'
    return state_name

def mongo_connect(db_name, collection_name):
    '''
    Connects to MongoDB database
    Note: change info in mongodb_credentials.py file
    (depending on deployment in dev or production environment)
    '''
    client = MongoClient(host=MDB_HOST, port=MDB_PORT)
    collection_name = getattr(client, db_name)
    tweets = collection_name.tweet
    logging.critical('Connected to MongoDB database %s\n*\n*\n*', db_name)
    return tweets

def postgres_connect():
    '''
    Connects to postgres database
    '''
    db_pg_string = f'postgres://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}'
    db_pg = create_engine(db_pg_string, echo=True)
    logging.critical('\n*\n*\n*Connected to Postgres database\n*\n*\n*')
    return db_pg

def create_table(table_name, db_pg):
    '''
    Creates table in Postgres database
    '''
    create_table_query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
    tweet_ID VARCHAR(50), username VARCHAR(50), text TEXT, clean_text TEXT, \
    handles TEXT, hashtags TEXT, followers_count BIGINT, \
    was_retweeted VARCHAR(50), loc_lat VARCHAR(50), loc_lon VARCHAR(50), \
    loc_type VARCHAR(50), location VARCHAR(50), in_us VARCHAR(50), \
    us_state VARCHAR(50), politician VARCHAR(50), date DATE, time VARCHAR(15), \
    date_hour VARCHAR(15), sentiment REAL, extracted VARCHAR(5) );"""
    logging.critical('\n*\n*\n--- Creating table in Postgres database ---\n*\n*\n*')
    db_pg.execute(create_table_query)

def extract(collection_name):
    '''
    Extract previously unextracted tweets from MongoDB database
    Marks them as extracted
    '''
    logging.critical('\n*\n*\n--- Extracting tweets ---\n*\n*\n*')
    extracted_tweets = list(collection_name.find({'extracted': 'no'}))
    logging.critical('\n*\n*\n--- Found %s tweets to extract ---\n*\n*\n*', len(extracted_tweets))
    for tweet in extracted_tweets:
        collection_name.update_one({'tweet_ID': tweet['tweet_ID']}, {'$set' : {'extracted' : 'yes'}})
    return extracted_tweets

def get_date_and_time(tweet):
    '''
    Splits Twitter 'created_by' field into date and time
    '''
    if isinstance(tweet, str):
        date = datetime.strptime(str(tweet), '%a %b %d %H:%M:%S %z %Y').date()
        tweet_time = datetime.strptime(str(tweet), '%a %b %d %H:%M:%S %z %Y').time()
    else:
        date = datetime.strptime(str(tweet), '%Y-%m-%d %H:%M:%S').date()
        tweet_time = datetime.strptime(str(tweet), '%Y-%m-%d %H:%M:%S').time()
    date_hour = str(date) + ' ' + str(tweet_time)
    date_hour = date_hour[:-6]
    return date, tweet_time, date_hour

def get_handles_hashtags(text):
    '''
    Returns a list of hashtags and user handles in the text
    '''
    handles = re.findall('\B\@\w+', text)
    hashtags = re.findall('\B\#\w+', text)
    return handles, hashtags

def clean_text(text, handles, hashtags):
    '''
    Returns text that has been tokenised and stripped of handles, hashtags, RT abbreviations and URLs
    '''
    combined = handles + hashtags + ['RT', 'amp', '\n']
    doc = nlp(text)
    stripped = [token.orth_ for token in doc if not token.is_punct]
    cleaned = [str(word) for word in stripped if word not in combined and not word.startswith('https')]
    cleaned = ' '.join(cleaned)
    return cleaned

def analyse_sentiment(tweet):
    '''
    Simple lexical approach to sentiment analysis using VADER
    Returns compound sentiment score
    '''
    sentiment = SENT_ANALYSIS.polarity_scores(tweet)
    return sentiment['compound']

def transform(extracted_tweets):
    '''
    Transform data and return sentiment analysis
    '''
    transformed_tweets = []
    for tweet in extracted_tweets:
        tweet['handles'], tweet['hashtags'] = get_handles_hashtags(tweet['text'])
        tweet['clean_text'] = clean_text(tweet['text'], tweet['handles'], tweet['hashtags'])
        tweet['sentiment'] = analyse_sentiment(tweet['clean_text'])
        tweet['date'], tweet['time'], tweet['date_hour'] = get_date_and_time(tweet['timestamp'])
        tweet['in_us'] = get_state(tweet, USA_DICT)
        tweet['us_state'] = get_state(tweet, STATE_DICT)
        transformed_tweets.append(tweet)
    return transformed_tweets


def load(transformed_tweets, pg_table_name, db_pg):
    '''
    Load transformed data into postgres database
    Takes db name as string
    '''
    for tweet in transformed_tweets:
        insert_query = f"INSERT INTO {pg_table_name} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        db_pg.execute(insert_query, (tweet['tweet_ID'], tweet['username'], tweet['text'], tweet['clean_text'], tweet['handles'], tweet['hashtags'], tweet['followers_count'], tweet['was_retweeted'], tweet['loc_lat'], tweet['loc_lon'], tweet['loc_type'], tweet['location'], tweet['in_us'], tweet['us_state'], tweet['politician'], tweet['date'], tweet['time'], tweet['date_hour'], tweet['sentiment'], tweet['extracted']))
        logging.critical('*\n*\n--- Inserting new tweet into postgres ---\n*\n*')

def execute_updates(drop_query, table_query, agg_query, db_pg):
    '''
    Bundles update execution
    '''
    db_pg.execute(drop_query)
    db_pg.execute(table_query)
    db_pg.execute(agg_query)

def update_sentiment_table(pg_table_name, db_pg):
    '''
    Updates aggregate table of mean sentiment per date_hour and politician in Postgres database
    '''
    drop_query = f'''DROP TABLE IF EXISTS agg_sentiment;'''
    table_query = f'''CREATE TABLE agg_sentiment (avg REAL, \
    date_hour VARCHAR(50), politician VARCHAR(50) );'''
    agg_query = f'''INSERT INTO agg_sentiment SELECT AVG(sentiment), \
    date_hour, politician FROM {pg_table_name} GROUP BY date_hour, politician \
    ORDER BY date_hour;'''
    execute_updates(drop_query, table_query, agg_query, db_pg)
    logging.critical('\n*\n*\n--- Updated table agg_sentiment ---\n*\n*')

def update_states_table(pg_table_name, db_pg):
    '''
    Updates aggregate table of sentiment by politician, state in Postgres database
    '''
    drop_query = f'''DROP TABLE IF EXISTS agg_state_sentiment;'''
    table_query = f'''CREATE TABLE agg_state_sentiment (avg REAL, \
    us_state VARCHAR(50), date VARCHAR(20), politician VARCHAR(50) );'''
    agg_query = f'''INSERT INTO agg_state_sentiment SELECT AVG(sentiment), \
    us_state, date, politician FROM {pg_table_name} GROUP BY  politician, \
    date, us_state ORDER BY date;'''
    execute_updates(drop_query, table_query, agg_query, db_pg)
    logging.critical('*\n*\n--- Updated table agg_state_sentiment ---\n*\n*')

def update_states_no_politicians(pg_table_name, db_pg):
    '''
    Updates aggregate table of sentiment by state in Postgres database (no segmentation by politician!)
    '''
    drop_query = f'''DROP TABLE IF EXISTS agg_noticket_state_sentiment;'''
    table_query = f'''CREATE TABLE agg_noticket_state_sentiment (avg REAL, \
    us_state VARCHAR(50) );'''
    agg_query = f'''INSERT INTO agg_noticket_state_sentiment \
    SELECT AVG(sentiment), us_state FROM {pg_table_name} GROUP BY us_state;'''
    execute_updates(drop_query, table_query, agg_query, db_pg)
    logging.critical('*\n*\n--- Updated table agg_noticket_state_sentiment ---\n*\n*')

def update_counts_table(pg_table_name, db_pg):
    '''
    Updates aggregate table of sentiment by date_hour, politician, state in Postgres database
    '''
    drop_query = f'''DROP TABLE IF EXISTS agg_count;'''
    table_query = f'''CREATE TABLE agg_count (count BIGINT, date VARCHAR(20), \
    politician VARCHAR(50) );'''
    agg_query = f'''INSERT INTO agg_count SELECT COUNT(*), date, politician \
    FROM {pg_table_name} GROUP BY date, politician ORDER BY date;'''
    execute_updates(drop_query, table_query, agg_query, db_pg)
    logging.critical('*\n*\n--- Updated table agg_count ---\n*\n*')

def setup():
    '''
    Set up connections and sleep time
    '''
    postgres_tweets = mongo_connect('tweet_mongodb', 'tweet_db')
    db_pg = postgres_connect()
    create_table('tweet_pg', db_pg)
    return postgres_tweets, db_pg

def main():
    '''
    All systems go! Extract, transform and load new tweets every 10 minutes.
    '''
    postgres_tweets, db_pg = setup()
    while True:
        extracted_tweets = extract(postgres_tweets)
        transformed_tweets = transform(extracted_tweets)
        load(transformed_tweets, 'tweet_pg', db_pg)
        update_sentiment_table('tweet_pg', db_pg)
        update_states_table('tweet_pg', db_pg)
        update_counts_table('tweet_pg', db_pg)
        update_states_no_politicians('tweet_pg', db_pg)
        logging.critical('... sleeping for 10 minutes')
        time.sleep(600)


#########
if __name__ == '__main__':
    main()
