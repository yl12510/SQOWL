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

import os
import sys

from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType
from datetime import datetime

if __name__ == "__main__":

    total_start = datetime.now() 

    if len(sys.argv) == 1: 
        print >> sys.stderr, "Please specify the transitive data file."
        exit(1)
    
    orgFile = sys.argv[1]

    sc = SparkContext(appName="PythonSQL")

    #sqlCtx = SQLContext(sc)

    sqlCtx = HiveContext(sc)


    if not os.path.isfile(orgFile):
        print "File does not exist."
    else: 
        lines = sc.textFile(orgFile)
        tc = lines.map(lambda x : (x.split(",")[0], x.split(",")[1])).distinct()


    schema = StructType([StructField("domain", StringType(), False), 
                         StructField("range", StringType(), False)])

    edges = sqlCtx.createDataFrame(tc,schema)
    tcSQL = sqlCtx.createDataFrame(tc,schema)

    tcSQL.cache()

    print "****** schema created ********"
    tcSQL.printSchema()

    orgCount = tcSQL.count()
    edges.registerAsTable("edges")
    sqlCtx.cacheTable("edges")

    print "******* Loop Start ********"

    loop_start = datetime.now()

    iteration = 0
    oldCount = 0L
    nextCount = tcSQL.count()

    while True:
        iteration = iteration + 1 
        
        print "****** Start iteration %i ******" % iteration

        
        oldCount = nextCount 

        tcTblName = "tcSQL" + str(1)

        tcSQL.registerAsTable(tcTblName)
        #sqlCtx.cacheTable(tcTblName)
        
        sqlStmt = "SELECT " + tcTblName + ".domain, edges.range FROM " + tcTblName + " JOIN edges ON " + tcTblName + ".range = edges.domain"

        
        new_edges = sqlCtx.sql(sqlStmt)

        #print "partition number for new_edges in iteration %i is %i" % (iteration,new_edges.getNumPartitions())

        tcSQL = tcSQL.unionAll(new_edges)

        #print "partition number for tc after union in iteration %i is %i" % (iteration,tcSQL.getNumPartitions())

        tcSQL = tcSQL.distinct()

        #print "partition number for tc after distinct in iteration %i is %i" % (iteration,tcSQL.getNumPartitions())

        tcSQL.cache()

        nextCount = tcSQL.count()

        if oldCount == nextCount: 
            break 

    loop_end = datetime.now()

    print "******* Loop End ********"

    finalCount = tcSQL.count()
    print "TC has %i edges" % finalCount
    
    sc.stop()

    total_end = datetime.now()



    print "******* Report ********"
    #print "The number of partitions used is %i: " % partitions
    print "TC was generated through %i joins" % iteration
    print "Original graph has %i " % orgCount
    print "Transitive Closure has %i " % finalCount
    print "New edges generated %i " % (finalCount - orgCount)
    print "Loop time: %i" % (loop_end - loop_start).total_seconds()
    print (loop_end - loop_start)
    print "Total time: %i" % (total_end - total_start).total_seconds()
    print (total_end - total_start)
    print "***********************"


    # RDD is created from a list of rows
    #some_rdd = sc.parallelize([Row(name="John", age=19),
    #                          Row(name="Smith", age=23),
    #                          Row(name="Sarah", age=18)])
    # Infer schema from the first row, create a DataFrame and print the schema
    #some_df = sqlContext.createDataFrame(some_rdd)
    #some_df.printSchema()

    # Another RDD is created from a list of tuples
    #another_rdd = sc.parallelize([("John", 19), ("Smith", 23), ("Sarah", 18)])
    # Schema with two fields - person_name and person_age
    #schema = StructType([StructField("person_name", StringType(), False),
    #                    StructField("person_age", IntegerType(), False)])
    # Create a DataFrame by applying the schema to the RDD and print the schema
    #another_df = sqlContext.createDataFrame(another_rdd, schema)
    #another_df.printSchema()
    # root
    #  |-- age: integer (nullable = true)
    #  |-- name: string (nullable = true)

    # A JSON dataset is pointed to by path.
    # The path can be either a single text file or a directory storing text files.
    #path = os.path.join(os.environ['SPARK_HOME'], "examples/src/main/resources/people.json")
    # Create a DataFrame from the file(s) pointed to by path
    #people = sqlContext.jsonFile(path)
    # root
    #  |-- person_name: string (nullable = false)
    #  |-- person_age: integer (nullable = false)

    # The inferred schema can be visualized using the printSchema() method.
    #people.printSchema()
    # root
    #  |-- age: IntegerType
    #  |-- name: StringType

    # Register this DataFrame as a table.
    #people.registerAsTable("people")

    # SQL statements can be run by using the sql methods provided by sqlContext
    #teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    #for each in teenagers.collect():
    #    print each[0]

    #sc.stop()
