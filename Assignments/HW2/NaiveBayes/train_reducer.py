#!/usr/bin/env python
"""
Reducer aggregates word counts by class and emits frequencies.

INPUT:
    partitionKey \t word \t class0_partialCount,class1_partialCount  
OUTPUT:
    word \t class0_totalCount,class1_totalCount,class0_conditionalProbability,class1_conditionalProbability
    
Instructions:
    Again, you are free to design a solution however you see 
    fit as long as your final model meets our required format
    for the inference job we designed in Question 8. Please
    comment your code clearly and concisely.
    
    A few reminders: 
    1) Don't forget to emit Class Priors (with the right key).
    2) In python2: 3/4 = 0 and 3/float(4) = 0.75
"""
##################### YOUR CODE HERE ####################

import sys
import numpy as np

current_word = None
current_word_total =  np.array([0,0])
word_total = np.array([0,0])
document_total = np.array([0,0])

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
        elif current_word:
            #print the current word and frequency
            print("{}\t{},{},{},{}".format(current_word,*current_word_total,*(current_word_total/word_total)))
        
        #reset current_word and current_word_total because a new word is encountered
        current_word = word
        current_word_total = partialCounts

#print the last record
print("{}\t{},{},{},{}".format(current_word,*current_word_total,*(current_word_total/word_total)))
##################### (END) CODE HERE ####################