__author__ = 'Eugene'
# -*- coding: utf-8 -*-

import csv
import hashlib
import collections
import string

def Statistics():
    with open("training_set.csv", "r") as csvfile:
        count_pos = 0
        count_neg = 0
        count_neu = 0

        reader = csv.DictReader(csvfile)
        for line in reader:

            if line["polarity"] == "positive":
                count_pos += 1
            elif line["polarity"] == "negative":
                count_neg += 1
            else:
                count_neu += 1

        total = count_neg+count_pos+count_neu
        print("Total records %d" % total)
        print("Negative %d: %f %%" % (count_neg, (float(count_neg)/total)*100))
        print("Positive %d: %f %%" % (count_pos, (float(count_pos)/total)*100))
        print("Neutral %d: %f %%" % (count_neu, (float(count_neu)/total)*100))


def most_imp(dict):
    importance = {}
    for ngram in dict:
        if str(ngram) in importance.keys():
            importance[str(ngram)] += 1
        else:
            importance[str(ngram)] = 1
    return importance


def sorted(mass):
    keylist = mass.keys()
    keylist.sort()
    for key in keylist:
        if mass[key]>1:
            print "%s: %s" % (key, mass[key])


def get_ngram(text, len):
    dict = []
    ngram = []
    count = 0
    for word in text:
        ngram.append(word)
        count += 1
        if count%len == 0 and count > 1:
            dict.append(ngram)
            ngram = []
            count = 0
    return (dict)

def transform(text):

    text = text.lower()
    text = text.split()

    for word in text:
        if word == "rt":
            text.remove(word)

    for word in text:
        if "@" in word:
            text.remove(word)

    for word in text:
        new_word = "".join(ch for ch in word if ch not in string.punctuation)
        text[text.index(word)] = new_word

    for word in text:
        if word is "" or '':
            text.remove(word)


    return text



data_sets = ["positive", "negative", "neutral"]


for set in data_sets:
    set_dict = []
    print("Working with set %s" % set)
    with open("training_set.csv", "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for line in reader:
            if line["polarity"] == set:
                print line["text"]
                text = transform(line["text"])
                print text
                print("\n")

                """
                dict = get_ngram(text.split(), 2)
                print (dict)

                for ngram in dict:
                    set_dict.append(ngram)
        importance = most_imp(set_dict)
        print sorted(importance)
        """