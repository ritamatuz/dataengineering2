import requests
import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


class Predictor:
    def __init__(self):
        self.model = None

    def initialize_model(self):
        self.model = SentimentIntensityAnalyzer()
  

    def predict(self, tweet: str):
        if self.model is None:
            self.initialize_model()

        api_url = "http://mymemory.translated.net/api/get?q={}&langpair={}|{}".format(tweet, "nl", "en")
        hdrs = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
            'Accept-Encoding': 'none',
            'Accept-Language': 'en-US,en;q=0.8',
            'Connection': 'keep-alive'}
        response = requests.get(api_url, headers=hdrs)
        response_json = json.loads(response.text)
        translation = response_json["responseData"]["translatedText"]
        
        score = self.model.polarity_scores(translation)["compound"]

        if score >= 0.05:
            sentiment = "positive"
        if score <= -0.05:
            sentiment = "negative"
        if score > -0.05 and score < 0.05:
            sentiment = "neutral"

        return {"sentiment": sentiment}
