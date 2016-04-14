__author__ = 'Eugene'
# -*- coding: utf-8 -*-

import csv
import hashlib
import collections


def Statistics():
    with open("training_set.csv", "r") as csvfile:
        count_pos = 0
        count_neg = 0
        count_neu = 0

        reader = csv.DictReader(csvfile)
        for line in reader:

            if line["polarity"] == "positive":
                count_pos+=1
            elif line["polarity"] == "negative":
                count_neg+=1
            else:
                count_neu+=1


        print("Negative %d" % count_neg)
        print("Positive %d" % count_pos)
        print("Neutral %d" % count_neu)


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



data_sets = ["positive", "negative", "neutral"]

"""
for set in data_sets:
    print("Working with set %s" % set)
    Learn(set)
    print("\n")
"""

for set in data_sets:
    class_dict = []
    print("Working with set %s" % set)
    with open("training_set.csv", "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for line in reader:
            if line["polarity"] == set:
                dict = get_ngram(line["text"].split(), 2)
                for ngram in dict:
                    class_dict.append(ngram)
        importance = most_imp(class_dict)
        print sorted(importance)