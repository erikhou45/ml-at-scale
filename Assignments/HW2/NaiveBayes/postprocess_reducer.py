#!/usr/bin/env python
"""
Reducer aggregates word counts by class and emits frequencies.

INPUT:
    pkey \t word \t class0_totalCount,class1_totalCount,class0_conditionalProbability(unsmoothed),class1_conditionalProbability(unsmoothed)
OUTPUT:
    word \t class0_totalCount,class1_totalCount,class0_conditionalProbability(smoothed),class1_conditionalProbability(smoothed)

"""
##################### YOUR CODE HERE ####################

import sys
import numpy as np

unique_word_count = np.array([0.0,0.0])
smooth_factor = np.array([1.0,1.0])

prior_rec = None

# read from standard input

# for line in sys.stdin:
# #     print('1')
#     print(line)

for line in sys.stdin:
#     print('1')
#     print(line)
    pKey, word, values = line.replace('\n','').split('\t')
    partialCounts = np.array(values.split(',')[:2]).astype("float")
#     class0_partialCount, class1_partialCount = values.split(',')
#     print("current_word={}, current_word_total={}, word={}, partialCounts={}".format(current_word, current_word_total, word, partialCounts))
    if word == "ClassPriors":
        prior_rec = "{}\t{}".format(word, values)
    elif word == "*total_count":
        total_word_count = partialCounts
    elif word == "*unique_count":
        unique_word_count += partialCounts
    elif word:
        #print the current word and frequency
        print("{}\t{},{},{},{}".format(word,*partialCounts,*((partialCounts+smooth_factor)/(total_word_count+unique_word_count))))

if prior_rec:
    print(prior_rec)
##################### (END) CODE HERE ####################
