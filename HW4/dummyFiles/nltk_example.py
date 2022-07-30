from nltk.sentiment import SentimentIntensityAnalyzer

sid = SentimentIntensityAnalyzer()

tweets = ["It is shameful the UK Government won’t condemn Trump.Now is the time to speak up for justice \
and equality. ", "How many unarmed blacks were killed by cops last year? 9. How many unarmed whites were \
killed by cops last year? 19. More", "Russia is invading Ukraine!"]

for tweet in tweets:
    score = sid.polarity_scores(tweet)
    print(f"Tweet : {tweet} \t Score : {score}")