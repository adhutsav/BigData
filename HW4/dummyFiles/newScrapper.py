import tweepy
from kafka import KafkaProducer
import sys
import json

def getConsumerKey():
    res = ""
    with open("keys/Bearer_token") as f:
        res += f.readline()
    return res

def getConsumerSecretKey():
    res = ""
    with open("keys/Bearer_token") as f:
        res += f.readline()
    return res

def getAccessToken():
    res = ""
    with open("keys/Bearer_token") as f:
        res += f.readline()
    return res

def getAccessTokenSecret():
    res = ""
    with open("keys/Bearer_token") as f:
        res += f.readline()
    return res
    
consumerKey = getConsumerKey()
consumerSecret = getConsumerSecretKey()
accessToken = getAccessToken()
accessTokenSecret = getAccessTokenSecret()

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], 
                            value_serializer=lambda x : json.dumps(x).encode('utf-8'))

def twitterAuth():
    authenticate = tweepy.OAuthHandler(consumerKey, consumerSecret)
    authenticate.set_access_token(accessToken, accessTokenSecret)
    api = tweepy.API(authenticate, wait_on_rate_limit=True)
    return api

class TweetListener(tweepy.Stream):

    def on_data(self, raw_data):
        topic_name = sys.argv[1]
        producer.send(topic_name, value=raw_data)
        return True

    def on_error(self, status_code):
        if status_code == 420:
            return False

    def start_streaming_tweets(self, search_term):
        self.filter(track=search_term, stall_warnings=True, languages=["en"])

if __name__ == '__main__':
    twitter_stream = TweetListener(consumerKey, consumerSecret, accessToken, accessTokenSecret)

    if len(sys.argv) != 3:
        print()
        print("Usage : <scriptName> <topic> <search_terms>")
        print()
        exit(1)

    topic_name = sys.argv[1]
    search_term = sys.argv[2].split(',')
    
    print(f"Search Terms : {search_term}")
    print(f"Topic : {topic_name}")
    #twitter_stream.start_streaming_tweets(search_term)

