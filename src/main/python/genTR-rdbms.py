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
    filename = "orgTR-" + str(numEdges) + ".sql"

    orgTR = open(filename,'w')

    orgTR.write("USE lubm_sb" + "\n" + "GO" + "\n")
    
    orgTR.write("DECLARE @EndTime datetime" + "\n" +
                "DECLARE @StartTime datetime" + "\n" + 
                "SELECT @StartTime=GETDATE()" + "\n" + 
                "BEGIN TRANSACTION" + "\n" + 
                "INSERT INTO transf.locatedIn(domain,range)" + "\n")

    i = 0
    while i < numEdges:
        src = rand.randrange(0, numEdges)
        dst = rand.randrange(0, numEdges)

        if src != dst:
            i += 1
            orgTR.write("SELECT 'individual" + str(src) + "','" + 
                        "individual" + str(dst)+"' ")
            
            if i < numEdges:
                orgTR.write("UNION\n")
            else:
                orgTR.write("\n")
            

    orgTR.write("COMMIT TRANSACTION" + "\n" + 
                "SELECT @EndTime=GETDATE()" + "\n" + 
                "SELECT DATEDIFF(ms,@StartTime,@EndTime) AS [Duration in millisecs]")

    # Close the file 
    orgTR.close        

    #stop = timeit.default_timer()
    stop = datetime.now()
    
    print "***************** Report *********************"
    print "The time used for generating " + filename + " is %i " % (stop - start).total_seconds()
    print (stop - start)
