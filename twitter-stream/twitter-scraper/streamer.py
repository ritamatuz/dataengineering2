import os
import twint


class TwitterPartyStreamer:
    def __init__(self):
        self.temp = "temp.csv"
        self.stream = "/home/jovyan/data/stream.csv"
        self.resume = "resume.txt"
        self.stream_columns = ["id", "created_at", "timezone", "user_id", "tweet", "hashtags"]
        self.parties = [
            'VVD', 'D66', 'PVV', 'PartijVoorDeVrijheid', 'CDA', 'SP', 'PvdA', 'Groenlinks',
            'FvD', 'ForumVoorDemocratie', 'PvdD', 'PartijVoorDeDieren' 'Christenunie',
            'Volt', 'JA21', 'SGP', 'DENK', '50Plus', 'BBB', 'BoerBurgerBeweging', 'Bij1',
        ]

    def configure_twint(self):
        config = twint.Config()
        config.Limit = 300
        config.Lang = "nl"
        config.Count = True
        config.Store_csv = True
        config.Hide_output = True
        config.Output = self.temp
        config.Near = "Amsterdam"
        config.Resume = self.resume
        config.Search = " OR ".join(["#" + party for party in self.parties])
        config.Custom["tweet"] = self.stream_columns

        return config

    def get_last_streamed_tweet(self):
        try:
            with open(self.stream, "rb") as file:
                # Go to the end of the file before the last break-line
                file.seek(-2, os.SEEK_END)
                # Keep reading backward until you find the next break-line
                while file.read(1) != b'\n':
                    file.seek(-2, os.SEEK_CUR)
                return file.readline().decode().split(",")
        except (FileNotFoundError, OSError):
            with open(self.stream, "w") as file:
                file.write(",".join(self.stream_columns) + "\n")
            return []

    def ingest(self):
        # Configure Twint scraper
        config = self.configure_twint()

        # Retrieve the last streamed id
        last_tweet = self.get_last_streamed_tweet()
        last_id = 0 if not last_tweet else int(last_tweet[0])

        # Scrape party tweets and store in temporary file
        twint.run.Search(config)

        # Open the temporary file for reading
        with open(self.temp, "r", encoding="utf8") as f_temp:

            # Open the stream for appending
            with open(self.stream, "a", encoding="utf8") as f_stream:

                # Iterate over the scraped tweets in reversed order
                for i, row in enumerate(reversed(list(f_temp)[1:])):

                    # Append new tweets to the stream
                    if int(row.split(",")[0]) > last_id:
                        f_stream.write(row)
