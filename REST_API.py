__author__ = 'Eugene'
# -*- coding: utf-8 -*-

import tweepy
import csv
import hashlib
import os.path
import time
import urllib
from text_to_vec import transform

consumer_key = ""
consumer_secret = ""


auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
api = tweepy.API(auth)

maxTweets = 10000000

pos_url='http://www.unc.edu/~ncaren/haphazard/positive.txt'
neg_url='http://www.unc.edu/~ncaren/haphazard/negative.txt'

urllib.urlretrieve(pos_url, 'positive.txt')
urllib.urlretrieve(neg_url, 'negative.txt')

pos_sent = open("positive.txt").read()
positive_words = pos_sent.split('\n')
neg_sent = open("negative.txt").read()
negative_words = neg_sent.split('\n')


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
    d = time.strptime(date, '%a %b %d %H:%M:%S +0000 %Y')
    d = time.strftime('%Y-%m-%d %H:%M:%S', d)
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

                text = tweet._json["text"].split()
                if text[0]=='RT' and "@" in text[1]:
                    del text[:2]
                    final_text = " ".join(text)
                else:
                    final_text = " ".join(text)

                hash = hashlib.md5(final_text.encode("utf-8")).hexdigest()
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

                    text = transform(data["text"])
                    positive = 0
                    negative = 0

                    for word in text:
                        if word in positive_words:
                            positive += 1
                        elif word in negative_words:
                            negative += 1

                    if positive == 0 and negative == 0 or positive == negative:
                        data["polarity"] = "neutral"
                    elif positive > negative:
                        data["polarity"] = "positive"
                    else:
                        data["polarity"] = "negative"


                    if data["polarity"] in ["positive", "negative"]:
                        try:
                            writer.writerow(data)
                            tweetCount += 1
                        except:
                            undecodable += 1
                    else:
                        print("unrecognized polarity")

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


get_tweets("weather")
