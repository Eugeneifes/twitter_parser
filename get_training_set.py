__author__ = 'Eugene'
# -*- coding: utf-8 -*-



import tweepy
import csv
import hashlib
import sys
import pprint
import os.path


consumer_key = "Ua4agkQ0Iflp0snjgNwtLUpe0"
consumer_secret = "eUYfxugm5G3NmbygZB0V90WlKVv3K5r90aUwys8ktjUmgAgLP9"
access_token = "715816338103656448-uN1IjZ7rLHxzbww3f9LAmEEoKztQqva"
access_token_secret = "3dGDI5jhUOI1w8nhfBOnhhE1YNgKAmF2CZGVdSt2U5ov0"

auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
api = tweepy.API(auth)

maxTweets = 10000000
tweetCount = 0


def load_hashes():
    hash_set = []
    with open("training_set.csv", "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for line in reader:
            hash_set.append(line["hash"])
    print("%s hashes loaded" % len(hash_set))
    return hash_set


def get_min_id():
    with open("training_set.csv", "r") as csvfile:
        min = 1000000000000000000000000000
        reader = csv.DictReader(csvfile)
        for line in reader:
            if int(line["id"])<min:
                min = int(line["id"])
        return min


def presetting():
    #Check database existance
    if os.path.isfile("training_set.csv"):
        hash_set = load_hashes()
        tweetCount = len(hash_set)
        last_id = get_min_id()
    else:
        print("no database")
        hash_set = []
        with open("training_set.csv", "w") as csvfile:
            fieldnames = ["id", "text", "hash", "polarity"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            tweetCount = 0
            last_id = None
    return (tweetCount, last_id, hash_set)



def get_tweets(query):

    tweetCount, max_id, hash_set = presetting()

    print("\n")
    print("Presetting:")
    print("Tweets in database: %d" % tweetCount)
    print("Last id: %s" % max_id)
    print("\n")

    #While not enough tweets in database
    while tweetCount < maxTweets:

        duplicates = 0
        undecodable = 0

        if max_id is None:
            tweets = api.search(q=query, count=100, lang="en")
        else:
            tweets = api.search(q=query, count=100, lang="en", max_id=str(max_id-1))
        print("%s tweets in queue" % len(tweets))

        if len(tweets)==0:
            print("No tweets for such query")
            break


        with open("training_set.csv", "a") as csvfile:
            fieldnames = ["id", "text", "hash", "polarity"]
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
                    if ":)" in data["text"]:
                        data["polarity"] = "positive"
                    elif ":(" in data["text"]:
                        data["polarity"] = "negative"

                    try:
                        writer.writerow(data)
                    except:
                        undecodable += 1

            max_id = tweets[-1].id

            print("\n")
            print("Statistics per step:")
            print("%d duplicates" % duplicates)
            print("%d undecodable" % undecodable)
            print("%d new tweets" % (len(tweets)-duplicates-undecodable))
            print("\n")

            if (len(tweets)-duplicates-undecodable) == 0:
                print("Can't get any new tweets")
                break

get_tweets(":) OR :(")
