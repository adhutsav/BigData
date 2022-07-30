import sys
import json
import requests
from kafka import KafkaProducer
import re

def getBearerToken():
    res = ""
    with open("keys/Bearer_token") as f:
        res += f.readline()
    return res

def bearer_oauth(r):
    r.headers["Authorization"]=f"Bearer {getBearerToken()}"
    r.headers["User-Agent"]= "v2FilteredStreamPython"
    return r

def cleanTweet(tweet: str) -> str:
    tweet = re.sub(r'http\S+', '', str(tweet))
    tweet = re.sub(r'bit.ly/\S+', '', str(tweet))
    tweet = tweet.strip('[link]')

    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    #my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'
    #tweet = re.sub('[' + my_punctuation + ']+', ' ', str(tweet))

    #tweet = re.sub('([0-9]+)', '', str(tweet))

    #tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    tweet = re.sub(' +', ' ', str(tweet))
    tweet = re.sub('\\n', '', str(tweet))
    tweet = re.sub('#', '', str(tweet))
    tweet = re.sub(':', '', str(tweet))
    tweet = tweet.strip()
    return tweet

def getKafkaProcducer():
    prod = None
    try:
        prod = KafkaProducer(bootstrap_servers=['localhost:9092'], 
                            value_serializer=lambda x : json.dumps(x).encode('utf-8'))
    except Exception as e:
        print("Couldn't connect to Kafka. Please check configuration.")
        print(str(e))
    finally:
        return prod

def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))

def set_rules(hash_tags):
    sample_rules = [
        {"value" : hash_tags+" lang:en"},
    ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))


def get_stream():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    kp = getKafkaProcducer()
    topic_name = sys.argv[1]
    
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            tweet = cleanTweet(json_response['data']['text'])
            if tweet:
                print(f"Tweet : {tweet}")
                kp.send(topic_name, value = tweet)
    
    
if __name__ == "__main__":
    length = len(sys.argv)

    if (length != 3):
        print("Please check following Usage:")
        print("Usage: <script_name> <topic-name> <comma_separated_hash_tags>")
        print()
        exit(1)
    
    hashTags = sys.argv[2].split(',')
    rules = get_rules()
    deleted_rules = delete_all_rules(rules)
    s_rules = set_rules(" OR ".join(hashTags))
    get_stream()
    
    


