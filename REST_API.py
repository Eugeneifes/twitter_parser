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


#Загрузка последнего состояния из БД
def get_last_state():
    hash_set = []
    id_set = []
    time_set = []

    with open("training_set.csv", "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for line in reader:
            hash_set.append(line["hash"])
            id_set.append(line["id"])
            time_set.append(line["date"])

    print("%s tweets loaded" % len(hash_set))
    minim = min(id_set)
    maxim = max(id_set)
    mintime = time_set[id_set.index(minim)]
    maxtime = time_set[id_set.index(maxim)]

    return hash_set, minim, maxim, mintime, maxtime


#конвертируем дату в удобочитаемый вид
def date_converter(date):

    d = datetime.strptime(date, '%a %b %d %H:%M:%S %z %Y')
    d = d.strftime('%Y-%m-%d %H:%M')
    return d


#подготовка начального состояния для загрузки
def presetting():
    #Check database existance
    if os.path.isfile("training_set.csv"):

        hash_set, minim, maxim, mintime, maxtime = get_last_state()
        tweetCount = len(hash_set)


        print("\n")
        print("Presetting:")
        print("Tweets in database: %d" % tweetCount)
        print("max id: %s" % maxim)
        print("max date: %s" % maxtime)
        print("min id: %s" % minim)
        print("min date: %s" % mintime)
        print("Distance between max and min ID: %d" % (int(maxim) - int(minim)))
        print("\n")

    else:
        print("no database")
        print("creating database")
        hash_set = []
        with open("training_set.csv", "w") as csvfile:
            fieldnames = ["id", "date", "text", "hash", "polarity"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            tweetCount = 0
            minim = None

    return (tweetCount, minim, hash_set)


def put_to_db(tweets, hash_set, duplicates, undecodable, tweetCount):
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
    return (duplicates, undecodable, tweetCount)


def get_tweets(query):

    tweetCount, minim, hash_set = presetting()

    #While not enough tweets in database
    while tweetCount < maxTweets:

        duplicates = 0
        undecodable = 0

        if minim is None:
            tweets = api.search(q=query, count=100, lang="en")
        else:
            tweets = api.search(q=query, count=100, lang="en", max_id=str(int(minim)-1))
        print("%s tweets in queue" % len(tweets))

        if len(tweets) == 0:
            print("No tweets for such query")
            break

        duplicates, undecodable, tweetCount = put_to_db(tweets, hash_set, duplicates, undecodable, tweetCount)


        print("\n")
        print("Statistics per step:")
        print("%d duplicates" % duplicates)
        print("%d undecodable" % undecodable)
        print("%d new tweets" % (len(tweets)-duplicates-undecodable))
        print("ongoing tweet count: %d" % tweetCount)
        print("\n")


        if (len(tweets)-duplicates-undecodable) == 0:
            print("Can't get any new tweets")
            
        minim = tweets[len(tweets)-1]._json["id"]


get_tweets(":) OR :(")
