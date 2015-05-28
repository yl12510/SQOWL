#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys
import os.path

from random import Random
from pyspark import SparkContext
from datetime import datetime


numEdges = 200
numVertices = 100
rand = Random(42)

# define a function to generate an original transitive graph
def generateGraph():
    edges = set()
    while len(edges) < numEdges:
        src = rand.randrange(0, numEdges)
        dst = rand.randrange(0, numEdges)
        if src != dst:
            edges.add((src, dst))
    return edges

if __name__ == "__main__":
    total_start = datetime.now()
    """
    Usage: transitive_closure [partitions]
    """

    if len(sys.argv) == 1:
        print >> sys.stderr, "Please specify the transitive file."
        exit(1)
        
    orgFile= sys.argv[1]
    partitions = int(sys.argv[2]) if len(sys.argv) > 2 else 1

    # define a spark context
    sc = SparkContext(appName="PythonTransitiveClosure")

    if not os.path.isfile(orgFile):
        print 'File does not exist.'
    else:
        lines = sc.textFile(orgFile)
        tc = lines.map(lambda x: (x.split(",")[0], x.split(",")[1])).distinct().cache()
        

        
    #print "The original transitive graph loaded from " + orgFile + " is: "
    #for line in tc.collect():
    #    print line

    
            
    org_count = tc.count()

    # manually define a transtive chain
    #data = [('a','b'),('b','c'),('c','d'),('d','e'),('e','f'),('f','g'),('g','h')]
    

    #tc = sc.parallelize(generateGraph(), partitions).cache()
    #tc = sc.parallelize(data, partitions).cache()

    
    # print out the original transitive closure
    # print "Original Transitive Closure"
    # for line in tc.collect():
    #    print line

    # Linear transitive closure: each round grows paths by one edge,
    # by joining the graph's edges with the already-discovered paths.
    # e.g. join the path (y, z) from the TC with the edge (x, y) from
    # the graph to obtain the path (x, z).

    # Because join() joins on keys, the edges are stored in reversed order.
    edges = tc.map(lambda (x, y): (y, x))

    print "******* Loop Start ********"
    loop_start = datetime.now()
    
    iteration = 0
    oldCount = 0L
    nextCount = tc.count()
    while True:
        iteration = iteration + 1
        print "****** Start iteration %i ******" % iteration
        oldCount = nextCount
        # Perform the join, obtaining an RDD of (y, (z, x)) pairs,
        # then project the result to obtain the new (x, z) paths.
        new_edges = tc.join(edges).map(lambda (_, (a, b)): (b, a))
        tc = tc.union(new_edges).distinct().cache()
        nextCount = tc.count()
        if nextCount == oldCount:
            break
        if iteration >= 2:
            break

    loop_end = datetime.now()    
    print "******* Loop End ********"

    # Define a filename
    #newFile = "comTC-" + orgFile

    #tcFile = open(newFile,'w')
    
    #print "The transitive closure generated is: "
    #for line in tc.sortByKey().collect():
    #    print line
        #tcFile.write(line + "\n")

    #tcFile.close()
    final_count = tc.count()
    print "TC has %i edges" % final_count
    sc.stop()

    total_end = datetime.now()    
        
    print "******* Report ********"
    #print "The number of partitions used is %i: " % partitions
    print "TC was generated through %i joins" % iteration
    print "Original graph has %i " % org_count
    print "Transitive Closure has %i " % final_count 
    print "New edges generated %i " % (final_count - org_count)
    print "Loop time: %i" % (loop_end - loop_start).total_seconds()
    print (loop_end - loop_start)
    print "Total time: %i" % (total_end - total_start).total_seconds()
    print (total_end - total_start)
    print "***********************"

    
