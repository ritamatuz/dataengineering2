import sys
import traceback
from flask import Flask, request, jsonify
from utils.sentiment import SentimentAnalyzer


app = Flask(__name__)


@app.route('/sentiment-api/analyze', methods=["POST"])
def ananlyze_tweet():
    try:
        # Get the tweet from the request body
        request_body = request.get_json()
        if not request_body:
            return jsonify({"error": "Please provide tweet as a JSON string."}), 400

        # Get the text input from the request body
        tweet = request_body.get('tweet', [''])

        # Make the predictions
        sentiment = sentiment_analyzer.analyze(tweet)
        status = 500 if "error" in sentiment else 200
        return jsonify(sentiment), status
    except:
        print(traceback.format_exc(), file=sys.stderr)
        return jsonify({"error": "Failed to analyze tweet... Check the error log."}), 500


sentiment_analyzer = SentimentAnalyzer()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
