import requests
from requests_oauthlib import OAuth1
import json
import urllib
import MySQLdb as mdb
import sys
from datetime import datetime, timedelta
import calendar

# Let's define consumer and access keys and secrets for getting access to Twitter API through your application
consumer_key = 'ex8R74QkZc2dSMnqa23UVCo00'
consumer_secret = 'w5eyduTNznYqEwyNy2hpuYNlrfrNGF3aSLPR9LETN1UKmNtx2H'
access_token = '796895774449733634-2HDV8yzn7BgfH5jM5kCVCdCQLAsKFHl'
access_secret = 'FUSPKORsxwdrlfDUWyr6fHXHK0DchqYXSNHMsVi8DoBo7'

# You will authenticate yourself using OAuth1 object
auth = OAuth1(consumer_key, consumer_secret, access_token, access_secret)
print(auth)

# If the authentication was successful, you should see the name of the account print(out)
url_1 = 'https://api.twitter.com/1.1/account/verify_credentials.json'
res = requests.get(url_1, auth=auth)

print("My name is", res.json()["name"])

query = '-RT "the internet is"'
encoded_query = urllib.parse.quote(query)

params = {
    "count": 100, 
    "lang": 'en',
    "q": encoded_query
}

url_2 = 'https://api.twitter.com/1.1/search/tweets.json'
res = requests.get(url_2, auth=auth, params=params)

print(res, res.status_code, res.headers['content-type'])
print(res.url)

tweets = res.json()

con = mdb.connect(host = 'localhost',
                      user = 'root', 
                      passwd = 'dwdstudent2015', 
                      charset = 'utf8', use_unicode=True);


def createDatabase():
    # Create the tweets database
    db_name = 'tweets_v8'
    create_db_query = "CREATE DATABASE IF NOT EXISTS {db} DEFAULT CHARACTER SET 'utf8'".format(db=db_name)

    # Create a database
    cursor = con.cursor()
    cursor.execute(create_db_query)
    cursor.close()


def createTable_tweets():
    cursor = con.cursor()
    db_name = 'tweets_v8'
    table_name = 'tweets'
    # Create a table to store tweets:
    create_table_query = '''CREATE TABLE IF NOT EXISTS {db}.{table} 
                                    (
                                    primary_id varchar(250),
                                    text varchar(250),
                                    PRIMARY KEY(primary_id)
                                    )'''.format(db=db_name, table=table_name)
    cursor.execute(create_table_query)
    cursor.close()


def createTable_metadata():
    cursor = con.cursor()
    db_name = 'tweets_v8'
    table_name = 'metadata'

    create_table_query = '''CREATE TABLE IF NOT EXISTS {db}.{table} 
                                    (
                                    primary_id varchar(250),
                                    tweet_id varchar(250),
                                    author varchar(250),
                                    date datetime,
                                    location varchar(250),
                                    PRIMARY KEY(primary_id),
                                    FOREIGN KEY(primary_id) 
                                            REFERENCES {db}.tweets(primary_id)
                                    )'''.format(db=db_name, table=table_name)
    cursor.execute(create_table_query)
    cursor.close()

def insertTweets(timestamp):
    query_template = '''INSERT IGNORE INTO tweets_v8.tweets(primary_id, text) 
                VALUES (%s, %s)'''

    cursor = con.cursor()
    
    timestamp = timestamp
    
    for num, tweet in enumerate(tweets['statuses']):
        
        primary_id = '-'.join([str(tweet['id']), str(timestamp)])
        text = tweet['text']

        query_parameters = (primary_id, text)
        cursor.execute(query_template, query_parameters)

    con.commit()
    cursor.close()

def insertMetadata(timestamp):
    query_template = '''INSERT IGNORE INTO tweets_v8.metadata(primary_id, tweet_id, author, date, location) 
                VALUES (%s, %s, %s, %s, %s)'''

    cursor = con.cursor()
    
    timestamp = timestamp
    
    for num, tweet in enumerate(tweets['statuses']):
        
        # Converting the date to EST timezone -- 'hacky' approximation:
        date_str = tweet['created_at']
        date =  datetime.strptime(date_str, '%a %b %d %H:%M:%S %z %Y')
        date = date - timedelta(hours=5)
        new_date = date.strftime('%Y-%m-%d %H:%M:%S')
        
        primary_id = '-'.join([str(tweet['id']), str(timestamp)])
        tweet_id = tweet['id']
        author = tweet['user']['screen_name']
        date = new_date
        location = tweet['user']['location']

        query_parameters = (primary_id, tweet_id, author, date, location)
        cursor.execute(query_template, query_parameters)

    con.commit()
    cursor.close()


# Current timestamp for the second part of the primary_id:
now = datetime.now()
timestamp = calendar.timegm(now.utctimetuple())

createDatabase()

createTable_tweets()
insertTweets(timestamp)

createTable_metadata()
insertMetadata(timestamp)