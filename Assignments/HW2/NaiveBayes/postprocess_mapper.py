#!/usr/bin/env python

"""

The postprocess mapper takes in the intermediary result from the last hadoop job and maps records to reducers.
The key function this mapper is performing is to duplicate total word count and unique word count so each partition will get a copy.
The mapper also evenly distribute other word records to the number of reducers used.

INPUT:
    word \t class0_totalCount,class1_totalCount,class0_conditionalProbability(unsmoothed),class1_conditionalProbability(unsmoothed)
OUTPUT:
    pkey \t word \t class0_totalCount,class1_totalCount,class0_conditionalProbability(unsmoothed),class1_conditionalProbability(unsmoothed)

"""


import sys                                                  
import numpy as np      

import os

if os.getenv('mapreduce_job_reduces') == None:
    N = 1
else:
    N = int(os.getenv('mapreduce_job_reduces'))
    
def makeKeyFile(num_reducers = N):
    # N = number of reducers
    KEYS = list(map(chr, range(ord('A'), ord('Z')+1)))[:num_reducers]
    partition_keys = sorted(KEYS)

    return partition_keys
    

pKeys = makeKeyFile()    
i = 0

for line in sys.stdin:                                    
    # parse input
    word, values = line.split('\t')
    
    if word == '*unique_count' or word == '*total_count':
        for pKey in pKeys:
            print('{}\t{}'.format(pKey, line.replace('\n','')))
    else:
        print('{}\t{}'.format(pKeys[i%N], line.replace('\n','')))
        