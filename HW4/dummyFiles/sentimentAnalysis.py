from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch
import sys
from nltk.sentiment import SentimentIntensityAnalyzer

def getElasticSearchKey():
    res = ""
    with open("keys/elasticSearchKey") as f:
        res += f.readline()

    return res

class SentimentAnalyser():
    def __init__(self):
        self.sid = SentimentIntensityAnalyzer()
    
    def classify(self, sentense):
        score = self.sid.polarity_scores(sentense)
        sentiment = ""
        if score['pos'] > score['neu'] > score['neg']:
            sentiment = 'pos'
        elif score['pos'] < score['neu'] < score['neg']:
            sentiment = 'neg'
        else:
            sentiment = 'neu'

        data = {
            'tweet' : sentense, 'sentiment':sentiment
        }

        return data

if __name__ == "__main__":
    length = len(sys.argv)
    if length != 2:
        print()
        print("Usuage <script> <comma seperated topic names>")
        print()
        exit(1)
    
    consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                             auto_offset_reset='earliest',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    
    subs_topic = sys.argv[1].split(',')
    consumer.subscribe(subs_topic)
    es = Elasticsearch(
            "https://localhost:9200",
            ca_certs="/Users/adhutsav/Downloads/elasticsearch-8.1.2/config/certs/http_ca.crt",
            basic_auth=("elastic", getElasticSearchKey())
        )

    sentiment_analyzer = SentimentAnalyser()
    for idx,message in enumerate(consumer):
        tweet_sentiment = sentiment_analyzer.classify(message.value)
        print(f"Message : {message.value} \t Class : {tweet_sentiment['sentiment']}")
        #es.index(index='tweetAnalysis', id=idx, document = tweet_sentiment)

    