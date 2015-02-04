package src.main.doc.ic.ac.uk.sqowl.rdbms;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class dbNoInf {

    // set which rdbms will be used (sqlserver as default)
    public static String dbType = "sqlServer";

    // set the database schema name (e.g. lubm, lubmNoInf)
    public static String dbSchema = "lubmNoInf";

    // initiate a map which is used to store certain sql statements
    // for each class or property
    public static Map<String, String> dbOWLTable;

    // to record the tBox reading time 
    public static long tBoxReadTime;

    // to record the tBox classification time by a tableaux-based
    // reasoner
    public static long tBoxClassificationTime;
    
    public static void main(String[] args){

	// connect to and setup database
        setupDB();
	
	
	System.out.println("sqowl");
    }

    public static void setupDB(){
	    // connect to and setup database
    private static void setupDB() throws SQLException {

        System.out.print("Step1: Database connecting......");
        //to specify variables for connecting to the corresponding database system
        String url= "", driver = "", password = "", user = "";

        if (dbType.equals("sqlServer")) {
            //ssh -L 12345:db.doc.ic.ac.uk:5432 shell1.doc.ic.ac.uk
            //url="jdbc:microsoft:sqlserver://localhost:1434";
            //url="jdbc:microsoft:sqlserver://db-ms.doc.ic.ac.uk:1433";
            //url="jdbc:microsoft:sqlserver://sqlserver.doc.ic.ac.uk:1433";
            url="jdbc:microsoft:sqlserver://magpie.doc.ic.ac.uk:1433";
            driver="com.microsoft.jdbc.sqlserver.SQLServerDriver";
            //password="UctId6Der";
            //user="yl12510_u";
            //password="BigSQOWL12";
            user="yl12510";
            password="yl12510";
            //password="WeikisBaj4";
            //user="ww312_u";
        }

        if(dbType.equals("postgresql")){
            url="jdbc:postgresql://localhost:12345/yl12510";
            driver="org.postgresql.Driver";
            password="6tszYXAaEt";
            user="yl12510";
        }

        dbConnection dbCon = new dbConnection(url, user, password, driver);

        // connect to the account
        dbCon.connectDb();

        // first set up the database lubm
        String dbUsed = "lubm";
        dbCon.setDBUsed(dbUsed);

        // clean the database, drop tables, views, functions, constraints
        dbCon.setupDB(dbSchema);


        // second, set up the database yu
        dbUsed = "Yu";
        dbCon.setDBUsed(dbUsed);

        // clean the database, drop only views
        dbCon.dropAllViews();

        System.out.println("Done!");
    }

    }
}
