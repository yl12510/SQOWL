import sys
#import timeit

from datetime import datetime
from random import Random



if __name__ == "__main__":

    #start = timeit.default_timer()
    start = datetime.now()
    
    if len(sys.argv) == 1:
        print >> sys.stderr, "Please specify the number of edges."
        exit(1)

    numEdges = int(sys.argv[1]) 
    numVertices = 100
    rand = Random(42)

    # Define a filename
    filename = "orgTR-" + str(numEdges) + ".csv"

    orgTR = open(filename,'w')

    i = 0

    while i < numEdges:
        src = rand.randrange(0, numEdges)
        dst = rand.randrange(0, numEdges)

        if src != dst:
            orgTR.write("individual" + str(src) + "," + "individual" + str(dst)+"\n")
            i += 1
        

    # Close the file 
    orgTR.close        

    #stop = timeit.default_timer()
    stop = datetime.now()
    
    print "***************** Report *********************"
    print "The time used for generating " + filename + " is %i " % (stop - start).total_seconds()
    print (stop - start)
