# imports
import re
import ast
import time
import numpy as np     

import pyspark

sc = pyspark.SparkContext()

testRDD = sc.textFile('gs://w261-bucket-hou/notebooks/HW5/data/test_graph.txt')
wikiRDD = sc.textFile('gs://w261-bucket-hou/notebooks/HW5/data/wiki_graph.txt')


def initGraph(dataRDD):
    """
    Spark job to read in the raw data and initialize an 
    adjacency list representation with a record for each
    node (including dangling nodes).
    
    Returns: 
        graphRDD -  a pair RDD of (node_id , (score, edges))
        
    NOTE: The score should be a float, but you may want to be 
    strategic about how format the edges... there are a few 
    options that can work. Make sure that whatever you choose
    is sufficient for Question 8 where you'll run PageRank.
    """
    ############## YOUR CODE HERE ###############

    # write any helper functions here
    def extractNodes(line):
        """
        Helper function to parses each input record and create a new record for any target nodes 
        and set it list of neighbors to be an empty set
        
        Returns: 
            a list of tuples of (key, value) - key is the node ID and value is either the adjacency list or a empty dictionary
        """
        sourceNode = line.split('\t')[0]
        edges = ast.literal_eval(line.split('\t')[1])

        yield (sourceNode,edges)

        for destNode in edges.keys():
            yield (destNode,{})


    def reduceRecords(rec_1, rec_2):
        """
        Helper function to reduce records by dropping the record proven to be an empty dictionary
        
        Returns: 
            a tuple of (key, value) - key is the node ID and value is either the adjacency list or a empty dictionary
        """
        if rec_1:
            return rec_1
        else:
            return rec_2
    
    # write your main Spark code here
    dataRDD = dataRDD.flatMap(lambda line: extractNodes(line)).reduceByKey(reduceRecords).cache()
    
    N = dataRDD.count() # get the number of total nodes
    
    graphRDD = dataRDD.mapValues(lambda x: (1.0/N, x)).cache() #initialize the mass of each node to 1/number of total nodes in the graph

    ############## (END) YOUR CODE ##############
    
    return graphRDD


#############################################################################################################

from pyspark.accumulators import AccumulatorParam

class FloatAccumulatorParam(AccumulatorParam):
    """
    Custom accumulator for use in page rank to keep track of various masses.
    
    IMPORTANT: accumulators should only be called inside actions to avoid duplication.
    We stringly recommend you use the 'foreach' action in your implementation below.
    """
    def zero(self, value):
        return value
    def addInPlace(self, val1, val2):
        return val1 + val2


def runPageRank(graphInitRDD, alpha = 0.15, maxIter = 10, verbose = True):
    """
    Spark job to implement page rank
    Args: 
        graphInitRDD  - pair RDD of (node_id , (score, edges))
        alpha         - (float) teleportation factor
        maxIter       - (int) stopping criteria (number of iterations)
        verbose       - (bool) option to print logging info after each iteration
    Returns:
        steadyStateRDD - pair RDD of (node_id, pageRank)
    """
    # teleportation:
    a = sc.broadcast(alpha)
    
    # damping factor:
    d = sc.broadcast(1-a.value)
    
    # initialize accumulators for dangling mass & total mass
    mmAccum = sc.accumulator(0.0, FloatAccumulatorParam())
    totAccum = sc.accumulator(0.0, FloatAccumulatorParam())
    
    ############## YOUR CODE HERE ###############
    
    # write your helper functions here, 
    # please document the purpose of each clearly 
    # for reference, the master solution has 5 helper functions.
        
        
    ###################################
    # MAPPER
    ###################################   
    
    def divideMass(row):
        """
        Helper function to create a new record for any target node and calculate the mass it was given from a source node. Also pass on the graph 
        
        Returns: 
            a list of tuples of (key, value) - key is the node ID and 
                                               value is either a tuple of (original mass, adjacency list, new mass) or 
                                                               a tuple of (0, mass passing on to a target node)
        """
        sourceNode = row[0]
        mass = row[1][0]
        edges = row[1][1]
        
        if edges!={}: #if the node is not a dangling node
            totalWeight = sum(edges.values())
            for targetNode in edges.keys():
                yield (targetNode, (0, mass/totalWeight * edges[targetNode])) #put a zero in the first position to flag this record as not part of the graph
        yield (sourceNode, (mass, edges, 0.0)) # pass on the graph
    
    def updateRank(value):
        """
        Helper function to calculate the new mass of a node 
        
        Returns: (old mass, adjacency list, new mass)
        """
        oldMass = value[0]
        edges = value[1]
        P = value[2]
        
        newMass = a.value/g.value + d.value*(dm.value/g.value+P)
        return (oldMass, edges, newMass)
    
    ###################################
    # REDUCER
    ###################################
    
    def collectMass(value1, value2):
        """
        Helper function to reduce records by summing the values of mass given by source nodes
        
        Returns: 
            a list of tuples of (key, value) - key is the node ID and 
                                               value is either a tuple of (original mass, adjacency list, new mass) or 
                                                               a tuple of (0, mass passing on to a target node)
        """
        if value1[0]==0 and value2[0]==0: #when both values are not a record of the graph. simply sum the given weights
            return (0, value1[1]+value2[1])
        elif value1[0]!=0: #if the first value is a record of the graph, we sum the given weights in the last position of the tuple
            return (value1[0], value1[1], value1[2] + value2[1])
        else: #if the second value is a record of the graph, we sum the given weights in the last position of the tuple
            return (value2[0], value2[1], value2[2] + value1[1])
    
    ###################################
    # ACCUMULATOR
    ###################################
    
    def collectDanglingMass(row):
        """
        Helper function to sum the mass from all the dangling nodes
        """
        sourceNode = row[0]
        mass = row[1][0]
        edges = row[1][1]
        
        if edges == {}: #if the node is a dangling node
            mmAccum.add(mass)
    
    def collectDistributedMass(row):
        """
        Helper function to sum the mass pass on the target nodes
        """
        totAccum.add(row[1][2])
    
    def terminate(row):
        """
        Helper function to sum the differences between the old mass and the new mass of each node
        """
        if row[1][0] != row[1][2]:  
            statusAccum.add(abs(row[1][0]-row[1][2]))
        
    # write your main Spark Job here (including the for loop to iterate)
    # for reference, the master solution is 21 lines including comments & whitespace
    
    nodeCount = graphInitRDD.count() # get the count of total nodes
    g = sc.broadcast(nodeCount) #broadcast node count to executors
    
    CONVERGENCE_THRESHOLD = 0.0 #set the convergence threshold to 0 since we are just trying to run a certain number iterations.
    
    for iteration in range(maxIter):
        #reset all the accumulators
        mmAccum = sc.accumulator(0.0, FloatAccumulatorParam())
        totAccum = sc.accumulator(0.0, FloatAccumulatorParam())
        statusAccum = sc.accumulator(0.0, FloatAccumulatorParam())
        
        #get the total mass of dangling nodes and broadcast it
        graphInitRDD.foreach(collectDanglingMass)
        dm = sc.broadcast(mmAccum.value)
        
        #start the main spark job of calculating new masses while preserving the graph 
        graphInitRDD = graphInitRDD.flatMap(divideMass).reduceByKey(collectMass).mapValues(updateRank).cache()
        
        #caculating the differences of between this iteration and last
        graphInitRDD.foreach(terminate)
        
        #output results for debugging
        if verbose:
            graphInitRDD.foreach(collectDistributedMass)
            print("-"*50)  
            print("iteration", iteration + 1)
            for i in graphInitRDD.collect():
                print(i)
            print("Total Mass Distributed: ", totAccum.value)
            print("Total Mass Diff:", statusAccum.value)
            print("-"*50) 
        
        #replace old mass with new mass
        graphInitRDD = graphInitRDD.mapValues(lambda value: (value[2], value[1])).cache()
        
        if statusAccum.value <= CONVERGENCE_THRESHOLD:
            break
    

 
    steadyStateRDD = graphInitRDD.mapValues(lambda value: value[0]).cache()
    ############## (END) YOUR CODE ###############
    
    return steadyStateRDD

# #comment out the code for testing
# nIter = 20
# testGraphRDD = initGraph(testRDD)
# start = time.time()
# test_results = runPageRank(testGraphRDD, alpha = 0.15, maxIter = nIter, verbose = False)
# print('...trained ',nIter, 'iterations in ',time.time() - start,' seconds.')
# print('Top 20 ranked nodes:')
# print(test_results.takeOrdered(20, key=lambda x: -x[1]))

#initialize graph
start = time.time()
wikiGraphRDD = initGraph(wikiRDD)

#run PageRank
nIter = 10
full_results = runPageRank(wikiGraphRDD, alpha = 0.15, maxIter = nIter, verbose = False)
print('...trained ',nIter, 'iterations in ',time.time() - start,' seconds.')
print('Top 20 ranked nodes:')
print(full_results.takeOrdered(20, key=lambda x: -x[1]))