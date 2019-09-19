#!/usr/bin/env python
"""
Reducer aggregates word counts by class and emits frequencies.

INPUT:
    partitionKey \t word \t class0_partialCount,class1_partialCount  
OUTPUT:
    word \t class0_totalCount,class1_totalCount,class0_conditionalProbability,class1_conditionalProbability
    
"""
##################### YOUR CODE HERE ####################

import sys
import numpy as np

current_word = None
current_word_total =  np.array([0,0])
word_total = np.array([0,0])
document_total = np.array([0,0])

#initialize the counts for unique words at -2 because we need to offset the increments from *docTotals and *wordTotals
unique_word_count = -2.0

# read from standard input
for line in sys.stdin:
    
    #split the line to get the componenets
    pkey, word, values = line.split('\t')
    #get the partial counts of the two classes
    partialCounts = np.array(values.replace('\n','').split(',')).astype("float")

    if word == current_word:
        current_word_total += partialCounts
    else:
        if word == "*wordTotals" and current_word == '*docTotals':
            document_total = current_word_total #set document total
            #print the prior here
            print("ClassPriors\t{},{},{},{}".format(*current_word_total,*(current_word_total/np.sum(current_word_total))))
        elif word != "*wordTotals" and current_word == '*wordTotals':
            word_total = current_word_total#set the word_total here
            print("*total_count\t{},{},{},{}".format(*word_total,0.0,0.0))
        elif current_word:
            #print the current word and frequency
            print("{}\t{},{},{},{}".format(current_word,*current_word_total,*(current_word_total/word_total)))
        
        #reset current_word and current_word_total because a new word is encountered
        current_word = word
        current_word_total = partialCounts
        #increment the count because a new word is encoutered
        unique_word_count += 1.0
        
#print the last record        
print("{}\t{},{},{},{}".format(current_word,*current_word_total,*(current_word_total/word_total)))
#print the unique count from all the words in a reducer
print("*unique_count\t{},{},{},{}".format(unique_word_count,unique_word_count,0.0,0.0))
##################### (END) CODE HERE ####################