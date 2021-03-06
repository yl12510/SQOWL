/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package src.main.doc.ic.ac.uk.sqowl.spark;
/**
 *
 * @author yl12510
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import scala.Tuple2;
import org.apache.spark.sql.SQLContext;

public class SqowlSpark {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        
        // Create a Java Spark Context 
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Sqowl Spark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // A Spark example for SubClassOf reasoning
        //subClassOfExp(sc);
        
        // A Spark example for SubClassOf, EquivalentClassOf, and Property domain and range reasoning
        //combinationExp(sc);
        
        // A Spark example for TransitiveProperty reasoning 
        //transitivePropertyExp(sc);
        
        // A Spark SQL example
        sparkSQLExp(sc);
              
    }
    
    private static void subClassOfExp(JavaSparkContext sc) {
        // Load input data 
        String dataFilePath = "src/data/csv/family/";
        
        // Load the original subclass and supclass instances as Java RDDs
        JavaRDD<String> man_org = sc.textFile(dataFilePath + "Man-1000000000.csv");
        JavaRDD<String> person_org = sc.textFile(dataFilePath + "Person-1000000000.csv");
        
        JavaRDD<String> person_inf = person_org.union(man_org);
        
        person_inf.saveAsTextFile(dataFilePath + "superClass.csv");
        
        
        /*
        System.out.println("--Person-1st--");
        for (String line : person_inf.collect()){
            System.out.println(line);
        }
         
        person_inf = person_inf.union(man_org);
        person_inf = person_inf.distinct();
        
        System.out.println("--Person-2nd--");
        for (String line : person_inf.collect()){
            System.out.println(line);
        } */
    }
    
    private static void transitivePropertyExp(JavaSparkContext sc) {
        
        // Load input data 
        String dataFilePath = "src/data/csv/transitive/";
        
        // Load the original transitive property instances as a Java RDD
        JavaRDD<String> lines = sc.textFile(dataFilePath + "ancestorOf.csv");
        
        // Transform the Java RDD to a Java Pair RDD
        PairFunction<String, String, String> keyData =
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String x){
                        return new Tuple2(x.split(" ")[0], x.split(" ")[1]);
                    }
                };
        
        PairFunction<String, String, String> invData =
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String x){
                        return new Tuple2(x.split(" ")[1], x.split(" ")[0]);
                    }
                };
        
 
        // Set an iteration variable
        //int i = 1;
        
        // 
        
        JavaPairRDD<String, String> trData = lines.mapToPair(keyData);
        JavaPairRDD<String, String> invTrData = lines.mapToPair(invData);

        JavaPairRDD<String, Tuple2<String, String>> tr = invTrData.join(trData);
        
        tr.saveAsTextFile(dataFilePath + "tr.txt");
        
    
    }

    private static void combinationExp(JavaSparkContext sc) {
        // Load input data
        String dataFilePath = "src/data/owl/family/";
        
        //JavaRDD<String> lines = sc.parallelize(Arrays.asList("John Mary", "Lewis Kate"));
        
        // Load property instances as Java RDDs
        JavaRDD<String> lines = sc.textFile(dataFilePath + "hasWife_org.csv");
        
        // Transform Java RDDs to Java Pair RDDs
        PairFunction<String, String, String> keyData =
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String x){
                        return new Tuple2(x.split(" ")[0], x.split(" ")[1]);
                    }
                };
        
        JavaPairRDD<String, String> hasWife = lines.mapToPair(keyData);
        
        // Inference from domain and range of properties 
        JavaRDD<String> man_inf = hasWife.keys();
        JavaRDD<String> woman_inf = hasWife.values();
        
        // Load class instances as Java RDDs
        JavaRDD<String> man_org = sc.textFile(dataFilePath + "Man_org.csv");
        JavaRDD<String> person_org = sc.textFile(dataFilePath + "Person_org.csv");
        JavaRDD<String> parent_org = sc.textFile(dataFilePath + "Parent_org.csv");
        JavaRDD<String> human_org = sc.textFile(dataFilePath + "Human_org.csv");
        
        // Perform inference from subclass rules 
        man_inf = man_inf.union(man_org).distinct();
        JavaRDD<String> parent_inf = parent_org;       
        JavaRDD<String> person_inf = person_org.union(man_inf);
        person_inf = person_inf.union(woman_inf);
        JavaRDD<String> human_inf = human_org.union(parent_inf);
        
        // Perform inference from equivalent rules 
        person_inf = person_inf.union(human_inf).distinct();
        human_inf = person_inf;
                
        // Print out the inference closure
        System.out.println("****Inference Closure*****");
        System.out.println("--Man--");
        for (String line : man_inf.collect()){
            System.out.println(line);
        }
        System.out.println("--Woman--");
        for (String line : woman_inf.collect()){
            System.out.println(line);
        }
        System.out.println("--Parent--");
        for (String line : parent_inf.collect()){
            System.out.println(line);
        }
        System.out.println("--Person--");
        for (String line : person_inf.collect()){
            System.out.println(line);
        }
        System.out.println("--Human--");
        for (String line : human_inf.collect()){
            System.out.println(line);
        }
        System.out.println("--hasWife--");
        for (Tuple2<String, String> line : hasWife.collect()){
            System.out.println(line);
        }
        
        
        // Save inference closure as text files 
        
        man_inf.saveAsTextFile(dataFilePath + "Man_inf.csv");
        woman_inf.saveAsTextFile(dataFilePath + "Woman_inf.csv");
        parent_inf.saveAsTextFile(dataFilePath + "Parent_inf.csv");
        person_inf.saveAsTextFile(dataFilePath + "Person_inf.csv");
        human_inf.saveAsTextFile(dataFilePath + "Human_inf.csv");
        
        
        hasWife.saveAsTextFile(dataFilePath + "hasWife_inf.csv");
    }

    private static void sparkSQLExp(JavaSparkContext sc) {
        
        // Create a Java Spark SQL Context 
        SQLContext sqlContext = new SQLContext(sc);
        
        /*
        // Create a sample DataFrame object 
        DataFrame df = sqlContext.jsonFile("/home/yl12510/spark-1.3.1-bin-hadoop2.6/examples/src/main/resources/people.json");
        
        df.show();
        df.printSchema();
        df.select("name").show();
        
        df.select(df.col("name"), df.col("age").plus(1)).show();
        
        df.filter(df.col("age").gt(21)).show(); 
        */
        JavaRDD<OWLProperty> transitive = sc.textFile("src/data/csv/transitive/trans_tree_2.csv").map(
                new Function<String, OWLProperty>() {
                    public OWLProperty call(String line) throws Exception{
                        String[] parts = line.split(" ");
                        OWLProperty owlPro = new OWLProperty();
                        owlPro.setDomain(parts[0]);
                        owlPro.setRange(parts[1]);
                        return owlPro;
                    }
                });
        
        
        DataFrame schemaOWLPro = sqlContext.createDataFrame(transitive, OWLProperty.class);
        schemaOWLPro.registerTempTable("transitive");
        
        /*DataFrame tr = sqlContext.sql("SELECT t1.domain, t2.range FROM transitive AS t1 JOIN transitive AS t2 ON t1.range = t2.domain");
        
        
        System.out.println("****before union*****");
        tr.show();
        
        long join_begin = System.currentTimeMillis();
        DataFrame tr_union = schemaOWLPro.unionAll(tr);
        long join_end = System.currentTimeMillis();
        
        System.out.println("****after union*****");
        tr_union.show();
        
        long distinct_begin = System.currentTimeMillis();
        DataFrame tr_union_distinct = tr_union.distinct();
        long distinct_end = System.currentTimeMillis();
        
        
        
        System.out.println("****after distinct*****");
        tr_union_distinct.show();*/
        
        // Set the interation variable for transitive loop
        int count = 0;
        long ins_Num = 0;
        DataFrame pre_Tr = null; 
        DataFrame new_Tr = null;
        DataFrame ins_Tr = null;
        
        
        do {
            
            if (count == 0) {
                pre_Tr = schemaOWLPro.distinct();
                ins_Tr = schemaOWLPro.distinct();
                

                pre_Tr.registerTempTable("pre_Tr");
                ins_Tr.registerTempTable("ins_Tr");
                
                String sqlStatement = "SELECT t1.domain, t2.range FROM pre_Tr AS t1 JOIN ins_Tr AS t2 ON t1.range = t2.domain";
                
                ins_Tr = sqlContext.sql(sqlStatement).distinct();
                new_Tr = pre_Tr.unionAll(ins_Tr).distinct();
                
            }
            else {
                pre_Tr = new_Tr;
                
                
                pre_Tr.registerTempTable("pre_Tr");
                ins_Tr.registerTempTable("ins_Tr");
                
                
                String sqlStatementA = "SELECT t1.domain, t2.range FROM pre_Tr AS t1 JOIN ins_Tr AS t2 ON t1.range = t2.domain";
                String sqlStatementB = "SELECT t1.domain, t2.range FROM ins_Tr AS t1 JOIN pre_Tr AS t2 ON t1.range = t2.domain";
                
                ins_Tr = sqlContext.sql(sqlStatementA).unionAll(sqlContext.sql(sqlStatementB)).distinct();
                new_Tr = pre_Tr.unionAll(ins_Tr).distinct();
            }
            
            System.out.println("Count is: " + count);
            count ++;
            
            ins_Num = ins_Tr.count();
        } while (ins_Num != 0);
        
        
        System.out.println("***************Original Transitive Property*****************");
        schemaOWLPro.show();
        System.out.println("***************Transitive Inference Closure*****************");
        new_Tr.show();
        
        
        System.out.println("***************Performance Report*****************");
        //System.out.println("Time used for JOIN: " + (join_end - join_begin)/1000 + "(s)");
        //System.out.println("Time used for DISTINCT: " + (distinct_end - distinct_begin)/1000 + "(s)");
        System.out.println("**************************************************");

    }
    
}
