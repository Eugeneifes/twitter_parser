__author__ = 'Eugene'
# -*- coding: utf-8 -*-



import tweepy
import csv
import hashlib
import os.path
from datetime import datetime


consumer_key = ""
consumer_secret = ""
access_token = ""
access_token_secret = ""

auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
api = tweepy.API(auth)

maxTweets = 10000000

happy = [":)", ":D", ":-)"]
sad = [":(", ":-("]


def load_hashes():
    hash_set = []
    with open("training_set.csv", "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for line in reader:
            hash_set.append(line["hash"])
    print("%s hashes loaded" % len(hash_set))
    return hash_set


def get_last_state():
    with open("training_set.csv", "r") as csvfile:
        last = csvfile.readlines()[-2]
        last = last.split(",")
        min = int(last[0])
        date = last[1]
        return (min, date)

def date_converter(date):

    d = datetime.strptime(date, '%a %b %d %H:%M:%S %z %Y')
    d = d.strftime('%Y-%m-%d %H:%M')
    return d


def presetting():
    #Check database existance
    if os.path.isfile("training_set.csv"):
        hash_set = load_hashes()
        tweetCount = len(hash_set)
        last_id, last_date = get_last_state()

        print("\n")
        print("Presetting:")
        print("Tweets in database: %d" % tweetCount)
        print("Last id: %s" % last_id)
        print("Last date: %s" % last_date)
        print("\n")

    else:
        print("no database")
        hash_set = []
        with open("training_set.csv", "w") as csvfile:
            fieldnames = ["id", "date", "text", "hash", "polarity"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            tweetCount = 0
            last_id = None

    return (tweetCount, last_id, hash_set)



def get_tweets(query):

    tweetCount, max_id, hash_set = presetting()

    #While not enough tweets in database
    while tweetCount < maxTweets:

        duplicates = 0
        undecodable = 0

        if max_id is None:
            tweets = api.search(q=query, count=100, lang="en")
        else:
            tweets = api.search(q=query, count=100, lang="en", max_id=str(int(max_id)-1))
        print("%s tweets in queue" % len(tweets))

        if len(tweets) == 0:
            print("No tweets for such query")
            break


        with open("training_set.csv", "a") as csvfile:
            fieldnames = ["id", "date", "text", "hash", "polarity"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            for tweet in tweets:

                hash = hashlib.md5(tweet._json["text"].encode("utf-8")).hexdigest()
                if hash in hash_set:
                    duplicates += 1
                    continue
                else:
                    hash_set.append(hash)
                    data = {}
                    data["id"] = tweet._json["id"]
                    data["text"] = tweet._json["text"]
                    data["hash"] = hash
                    data["date"] = date_converter(tweet._json["created_at"])

                    if any(smile in data["text"] for smile in happy):
                        data["polarity"] = "positive"
                    elif any(smile in data["text"] for smile in sad):
                        data["polarity"] = "negative"
                    else:
                        data["polarity"] = "neutral"

                    try:
                        writer.writerow(data)
                        tweetCount += 1
                    except:
                        undecodable += 1

            print("\n")
            print("Statistics per step:")
            print("%d duplicates" % duplicates)
            print("%d undecodable" % undecodable)
            print("%d new tweets" % (len(tweets)-duplicates-undecodable))
            print("ongoing tweet count: %d" % tweetCount)
            print("\n")

            if (len(tweets)-duplicates-undecodable) == 0:
                print("Can't get any new tweets")
                break


get_tweets(":) OR :(")
