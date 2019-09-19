#!/usr/bin/env python
"""
The postprocess reducer aggregates the unique word counts and calculate the smoothed conditional probability for each word.

INPUT:
    pkey \t word \t class0_totalCount,class1_totalCount,class0_conditionalProbability(unsmoothed),class1_conditionalProbability(unsmoothed)
OUTPUT:
    word \t class0_totalCount,class1_totalCount,class0_conditionalProbability(smoothed),class1_conditionalProbability(smoothed)

"""

import sys
import numpy as np

#initialize unique word count
unique_word_count = np.array([0.0,0.0])

#set the smooth factor
smooth_factor = np.array([1.0,1.0])


prior_rec = None

# read from standard input
for line in sys.stdin:
    
    #parse the input
    pKey, word, values = line.replace('\n','').split('\t')
    partialCounts = np.array(values.split(',')[:2]).astype("float")

    if word == "ClassPriors":
        #remember the prior for printing later
        prior_rec = "{}\t{}".format(word, values)
    elif word == "*total_count":
        #remember the total word count
        total_word_count = partialCounts
    elif word == "*unique_count":
        #sum the unique word count
        unique_word_count += partialCounts
    elif word:
        #print the current word and smoothed conditional probabilities
        print("{}\t{},{},{},{}".format(word,*partialCounts,*((partialCounts+smooth_factor)/(total_word_count+unique_word_count))))

#print the prior last so only one prior is printed. This doesn't affect the correctness of the result of the pipeline but it makes the model file cleaner
if prior_rec:
    print(prior_rec)

