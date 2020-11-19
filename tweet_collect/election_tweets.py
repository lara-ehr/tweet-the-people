import json
import logging
import time
import random
import datetime
import geocoder

from tweepy import OAuthHandler, Stream, API
from tweepy.streaming import StreamListener

from pymongo import MongoClient

from credentials import *

SEARCHTERMS = [['joe biden', 'joebiden'], ['kamala harris', 'kamalaharris'], \
 ['donald trump', 'donaldtrump'], ['mike pence', 'mikepence']]

def mongo_connect(db_name, collection_name):
    '''
    Connects to MongoDB database
    Note: change info in credentials.py file
    '''
    client = MongoClient(host=MDB_HOST, port=MDB_PORT)
    collection_name = getattr(client, db_name)
    tweets = collection_name.tweet
    logging.critical('Connected to MongoDB database %s\n*\n*\n*', db_name)
    return tweets


def authenticate():
    '''
    Handles twitter authentication.
    In this directory, store a script named credentials.py using the format:

    consumer_key = 'string with your consumer api key'
    consumer_secret = 'string with your consumer api secret'
    access_token = 'string with your access token'
    access_token_secret = 'string with your access token secret'
    '''
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return auth


def get_loc(raw_tweet):
    '''
    Returns:
    1. tweet location (with coordinates where possible),
    2. data source (self-reported, from geotagging etc.)
    Location selected ranked order of data quality
    - Geotagged data: preferred but rare
    - Self-reported location least preferred
    - No info returns string 'no_loc'
    Info about Twitter location data and structure:
    https://developer.twitter.com/en/docs/twitter-api/v1/data-dictionary/overview/geo-objects
    Notable data decisions:
    - coordinates in 'geo' are in order lat, lon
    - coordinates elsewhere are in order lon, lat
    - bounding_box: only 1st set of coordinates; sufficient for present purposes
    '''
    loc_lat = 'no_loc'
    loc_lon = 'no_loc'
    loc_type = 'no_loc'
    location = 'no_loc'
    if 'geo' in raw_tweet and raw_tweet['geo'] is not None:
        loc_lat = raw_tweet['geo']['coordinates'][0]
        loc_lon = raw_tweet['geo']['coordinates'][1]
        loc_type = 'geo_loc'
        location = 'coords'
    elif 'place' in raw_tweet and raw_tweet['place'] is not None and raw_tweet['place']['bounding_box'] is not None:
        loc_lon = raw_tweet['place']['bounding_box']['coordinates'][0][0][0]
        loc_lat = raw_tweet['place']['bounding_box']['coordinates'][0][0][1]
        loc_type = 'bound_box_coords'
        location = 'box_coords'
    elif 'place' in raw_tweet and raw_tweet['place'] is not None:
        pre = geocoder.arcgis(raw_tweet['place']['full_name'])
        location = raw_tweet['place']['full_name']
        if pre.ok:
            loc_lon = pre.x
            loc_lat = pre.y
            loc_type = 'place'
    elif 'location' in raw_tweet['user'] and raw_tweet['user']['location'] is not None:
        location = raw_tweet['user']['location'][:50]
        pre = geocoder.arcgis(raw_tweet['user']['location'])
        if pre.ok:
            loc_lon = pre.x
            loc_lat = pre.y
            loc_type = 'user_loc'
        else:
            location = 'no_loc'
    return loc_lat, loc_lon, loc_type, location


def get_retweet(raw_tweet):
    '''
    Returns retweet status and text of original tweet.
    If tweet does not contain text, text is returned as 'text_empty'
    (and these tweets get filtered out later)
    '''
    text = 'text_empty'
    if 'extended_tweet' in raw_tweet:
        text = raw_tweet['extended_tweet']['full_text']
    if 'retweeted_status' in raw_tweet:
        retweet = raw_tweet['retweeted_status']
        was_retweeted = 'true'
        if 'extended_tweet' in retweet:
            text = retweet['extended_tweet']['full_text']
    else:
        was_retweeted = 'false'
    return text, was_retweeted

class TwitterListener(StreamListener):
    '''
    Defines TwitterListener as an instance of StreamListener
    Additions for passing in API, politician, runtime and database
    '''

    def __init__(self, api, politician, runtime, mongo_database):
        self.api = api
        self.politician = politician
        self.start = time.time()
        self.runtime = runtime
        self.mongo_database = mongo_database

    def on_connect(self):
        logging.critical(
            '\n*\n*\n--- GETTING TWEETS ABOUT POLITICIAN %s FOR %s SECONDS ---\n*\n*\n',
            self.politician, self.runtime)

    def on_data(self, data):
        '''
        Extracts tweets with specified keywords.
        Stores them in a MongoDB database.
        '''
        now = time.time()
        if (now - self.start) < self.runtime:

            raw_tweet = json.loads(data)

            if 'user' in raw_tweet:

                text, was_retweeted = get_retweet(raw_tweet)

                loc_lat, loc_lon, loc_type, location = get_loc(raw_tweet)

                tweet = {
                    'text': text,
                    'username': raw_tweet['user']['screen_name'],
                    'followers_count': raw_tweet['user']['followers_count'],
                    'was_retweeted': was_retweeted,
                    'timestamp': raw_tweet['created_at'],
                    'tweet_ID': raw_tweet['id_str'],
                    'loc_lat': loc_lat,
                    'loc_lon': loc_lon,
                    'loc_type': loc_type,
                    'location': location,
                    'politician': self.politician,
                    'extracted': 'no'
                }

                tweet_id = tweet['tweet_ID']

                if tweet['text'] != 'text_empty' and self.mongo_database.find({'tweet_ID': tweet_id}).count() == 0:
                    self.mongo_database.insert_one(tweet)
                    logging.critical(
                        '\n*\n*\n--- LOGGED %s ABOUT %s---', str(tweet_id), self.politician)
                    logging.critical('--- Remaining runtime: %s seconds---\n*\n*\n*',
                                     round(self.runtime - (now - self.start), 2))

        else:
            logging.critical(
                '\n*\n*\n--- DISCONNECTING TWITTER STREAM ABOUT POLITICIAN %s ---\n*\n*\n', self.politician)
            return False

    def on_error(self, status):
        if status == 420:
            print(status)
            return False


def tweet_sleep():
    '''
    Pauses tweet collection for ca. 7 min.
    '''
    sleep = random.randint(50, 70) * random.randint(3, 8)
    # jitter for sleep: sleep duration is roughly 1 minute * roughly 7 minutes
    logging.critical('... sleeping for %s seconds', sleep)
    logging.critical('... sleeping at: %s---\n*\n*\n',
                     datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    time.sleep(sleep)

def get_runtime(politician):
    '''
    Sets runtime of tweet collection
    '''
    runtime_basic = 75
    # standard runtime is short to avoid timeouts due to too-rapid rate of incoming tweets
    # these can otherwise cause backlogs that overwhelm the data architecture
    if politician == 'mikepence':
        runtime_basic = 120
        # people tweet less about Mike Pence
        # setting runtime higher to get more data
    runtime_jitter = random.randint(8, 17)
    runtime = runtime_basic - runtime_jitter
    return runtime

def setup():
    '''
    Set up connection to database, authentication and API config
    '''
    mongo_collection_tweets = mongo_connect('tweet_mongodb', 'tweet_db')
    auth = authenticate()
    my_api = API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    return mongo_collection_tweets, auth, my_api

def get_tweets(i, auth, my_api, mongo_database):
    '''
    Stream tweets
    '''
    current_search = SEARCHTERMS[i]
    politician = current_search[1]
    runtime = get_runtime(politician)
    listener = TwitterListener(my_api, politician, runtime, mongo_database)
    stream = Stream(auth, listener)
    stream.filter(track=current_search, languages=['en'])

def main():
    '''
    All systems go!
    '''
    mongo_collection_tweets, auth, my_api = setup()
    i = 0
    while True:
        get_tweets(i, auth, my_api, mongo_collection_tweets)
        if i >= 3:
            i = 0
        else:
            i += 1
        tweet_sleep()

##########


if __name__ == '__main__':
    main()
