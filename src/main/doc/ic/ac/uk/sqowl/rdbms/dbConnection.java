package package src.main.doc.ic.ac.uk.sqowl.rdbms;

import java.sql.*;

/**
 * Created with IntelliJ IDEA.
 * User: yu
 * Date: 31/07/12
 * Time: 11:49
 * To change this template use File | Settings | File Templates.
 */
public class dbConnection {
    private String url, username, password, dbDriver;
    private Connection con = null;

    public dbConnection(String url, String username, String password, String dbDriver){
        this.url = url;
        this.username = username;
        this.password = password;
        this.dbDriver = dbDriver;
    }

    public void connectDb(){
        try {
            Class.forName(dbDriver);
            con = DriverManager.getConnection(url,username,password);
            DatabaseMetaData dbmd = con.getMetaData();
            System.out.println("Connection to "+dbmd.getDatabaseProductName()+" " +
                    dbmd.getDatabaseProductVersion()+dbmd.getURL()+" successful.\n");
        } catch (Exception e){
            System.err.println("Unable to connect to db: "+e.getMessage());
            System.exit(10);
        }
    }

    public void setupDB(String dbSchema) throws SQLException {
        /*String dropFK = "while(exists(select 1 from INFORMATION_SCHEMA.TABLE_CONSTRAINTS where CONSTRAINT_TYPE='FOREIGN KEY'))\n" +
                "begin\n" +
                "\tdeclare @sql nvarchar(2000)\n" +
                "\tSELECT TOP 1 @sql=('ALTER TABLE ' + TABLE_SCHEMA + '.[' + TABLE_NAME\n" +
                "\t+ '] DROP CONSTRAINT [' + CONSTRAINT_NAME + ']')\n" +
                "\tFROM information_schema.table_constraints\n" +
                "\tWHERE CONSTRAINT_TYPE = 'FOREIGN KEY'\n" +
                "\texec (@sql)\n" +
                "end";
        executeStatement(dropFK);*/
        /*String dropTable = "while(exists(select 1 from sysobjects where xtype ='U'))\n" +
                "begin\n" +
                "declare @tname varchar(8000)\n" +
                "set @tname=''\n" +
                "select @tname=@tname + Name + ',' from sysobjects where xtype='U'\n" +
                "select @tname='drop table ' + left(@tname,len(@tname)-1)\n" +
                "exec(@tname)\n"+
                "end";
        executeStatement(dropTable);
        
        String dropView = "declare @SQL nvarchar(max)\n"
        		+ "set @SQL = (select 'drop view '+name+'; ' from sys.views for xml path(''))\n"
        		+ "exec (@SQL)";
        executeStatement(dropView);*/
        
        
        /* Drop all non-system stored procs */
        String sqlStatement = "DECLARE @name VARCHAR(128)\n"
        		+ "DECLARE @SQL VARCHAR(254)\n"
        		+ "SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] = 'P' AND category = 0 ORDER BY [name])\n"
        		+ "WHILE @name is not null\n"
        		+ "BEGIN\n"
        		+ "SELECT @SQL = 'DROP PROCEDURE [dbo].[' + RTRIM(@name) +']'\n"
        		+ "EXEC (@SQL)\n"
        		+ "PRINT 'Dropped Procedure: ' + @name\n"
        		+ "SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] = 'P' AND category = 0 AND [name] > @name ORDER BY [name])\n"
        		+ "END";
        //executeStatement(sqlStatement);

        /* Drop all views */
        sqlStatement = "DECLARE @name VARCHAR(128)\n"
        		+ "DECLARE @SQL VARCHAR(254)\n"
        		+ "SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] = 'V' AND category = 0 ORDER BY [name])\n"
        		+ "WHILE @name IS NOT NULL\n"
        		+ "BEGIN\n"
        		+ "SELECT @SQL = 'DROP VIEW [dbo].[' + RTRIM(@name) +']'\n"
        		+ "EXEC (@SQL)\n"
        		+ "PRINT 'Dropped View: ' + @name\n"
        		+ "SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] = 'V' AND category = 0 AND [name] > @name ORDER BY [name])\n"
        		+ "END\n";
        //executeStatement(sqlStatement);

        /* Drop all functions */
        sqlStatement = "DECLARE @name VARCHAR(128)\n"
        		+ "DECLARE @SQL VARCHAR(254)\n"
        		+ "SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] IN (N'FN', N'IF', N'TF', N'FS', N'FT') AND category = 0 ORDER BY [name])\n"
        		+ "WHILE @name IS NOT NULL\n"
        		+ "BEGIN\n"
        		+ "SELECT @SQL = 'DROP FUNCTION [dbo].[' + RTRIM(@name) +']'\n"
        		+ "EXEC (@SQL)\n"
        		+ "PRINT 'Dropped Function: ' + @name\n"
        		+ "SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] IN (N'FN', N'IF', N'TF', N'FS', N'FT') AND category = 0 AND [name] > @name ORDER BY [name])\n"
        		+ "END\n";
        //executeStatement(sqlStatement);

        /* Drop all Foreign Key constraints */
        sqlStatement = "DECLARE @name VARCHAR(128)\n"
        		+ "DECLARE @constraint VARCHAR(254)\n"
        		+ "DECLARE @SQL VARCHAR(254)\n"
        		+ "SELECT @name = (SELECT TOP 1 TABLE_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'FOREIGN KEY' ORDER BY TABLE_NAME)\n"
        		+ "WHILE @name is not null\n"
        		+ "BEGIN\n"
        		+ "SELECT @constraint = (SELECT TOP 1 CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'FOREIGN KEY' AND TABLE_NAME = @name ORDER BY CONSTRAINT_NAME)\n"
        		+ "WHILE @constraint IS NOT NULL\n"
        		+ "BEGIN\n"
        		+ "SELECT @SQL = 'ALTER TABLE [dbo].[' + RTRIM(@name) +'] DROP CONSTRAINT [' + RTRIM(@constraint) +']'\n"
        		+ "EXEC (@SQL)\n"
        		+ "PRINT 'Dropped FK Constraint: ' + @constraint + ' on ' + @name\n"
        		+ "SELECT @constraint = (SELECT TOP 1 CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'FOREIGN KEY' AND CONSTRAINT_NAME <> @constraint AND TABLE_NAME = @name ORDER BY CONSTRAINT_NAME)\n"
        		+ "END\n"
        		+ "SELECT @name = (SELECT TOP 1 TABLE_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'FOREIGN KEY' ORDER BY TABLE_NAME)\n"
        		+ "END\n";
        //executeStatement(sqlStatement);

        /* Drop all Primary Key constraints */
        sqlStatement = "DECLARE @name VARCHAR(128)\n"
        		+ "DECLARE @constraint VARCHAR(254)\n"
        		+ "DECLARE @SQL VARCHAR(254)\n"
        		+ "SELECT @name = (SELECT TOP 1 TABLE_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'PRIMARY KEY' ORDER BY TABLE_NAME)\n"
        		+ "WHILE @name IS NOT NULL\n"
        		+ "BEGIN\n"
        		+ "SELECT @constraint = (SELECT TOP 1 CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'PRIMARY KEY' AND TABLE_NAME = @name ORDER BY CONSTRAINT_NAME)\n"
        		+ "WHILE @constraint is not null\n"
        		+ "BEGIN\n"
        		+ "SELECT @SQL = 'ALTER TABLE [dbo].[' + RTRIM(@name) +'] DROP CONSTRAINT [' + RTRIM(@constraint)+']'\n"
        		+ "EXEC (@SQL)\n"
        		+ "PRINT 'Dropped PK Constraint: ' + @constraint + ' on ' + @name\n"
        		+ "SELECT @constraint = (SELECT TOP 1 CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'PRIMARY KEY' AND CONSTRAINT_NAME <> @constraint AND TABLE_NAME = @name ORDER BY CONSTRAINT_NAME)\n"
        		+ "END\n"
        		+ "SELECT @name = (SELECT TOP 1 TABLE_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'PRIMARY KEY' ORDER BY TABLE_NAME)\n"
        		+ "END\n";
        //executeStatement(sqlStatement);

        /* Drop all tables */
        sqlStatement = "DECLARE @name VARCHAR(128)\n"
        		+ "DECLARE @SQL VARCHAR(254)\n"
        		+ "SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] = 'U' AND category = 0 ORDER BY [name])\n"
        		+ "WHILE @name IS NOT NULL\n"
        		+ "BEGIN\n"
        		+ "SELECT @SQL = 'DROP TABLE [dbo].[' + RTRIM(@name) +']'\n"
        		+ "EXEC (@SQL)\n"
        		+ "PRINT 'Dropped Table: ' + @name\n"
        		+ "SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] = 'U' AND category = 0 AND [name] > @name ORDER BY [name])\n"
        		+ "END\n";
        //executeStatement(sqlStatement);

        sqlStatement = "DECLARE @SqlStatement VARCHAR(MAX)\n" +
                "SELECT @SqlStatement = \n" +
                "COALESCE(@SqlStatement, '') + 'DROP TABLE [%schema].' + QUOTENAME(TABLE_NAME) + ';' + CHAR(13)\n".replaceAll("%schema", dbSchema) +
                "FROM INFORMATION_SCHEMA.TABLES\n" +
                "WHERE TABLE_SCHEMA = '%schema'\n".replaceAll("%schema", dbSchema) +
                "EXEC (@SqlStatement)";
        executeStatement(sqlStatement);

    }

    public void executeStatement(String sqlStatement) throws SQLException {
        System.out.println(sqlStatement);
        Statement stmt = con.createStatement();
        stmt.executeUpdate(sqlStatement);
        stmt.close();

    }

    public Statement createStatement() throws SQLException {
        return con.createStatement();
    }

    public void setDBUsed(String dbUsed) throws SQLException{
        String sqlStatement = "USE " + dbUsed;
        executeStatement(sqlStatement);
    }

    public void allowRecursiveTriggers(String dbUsed) throws SQLException{
        String sqlStatement = "ALTER DATABASE " + dbUsed +
                " SET RECURSIVE_TRIGGERS ON ";
        executeStatement(sqlStatement);
    }

    public void dropAllViews() throws SQLException{
        /* Drop all views */
        String sqlStatement = "DECLARE @name VARCHAR(128)\n"
                + "DECLARE @SQL VARCHAR(254)\n"
                + "SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] = 'V' AND category = 0 ORDER BY [name])\n"
                + "WHILE @name IS NOT NULL\n"
                + "BEGIN\n"
                + "SELECT @SQL = 'DROP VIEW [dbo].[' + RTRIM(@name) +']'\n"
                + "EXEC (@SQL)\n"
                + "PRINT 'Dropped View: ' + @name\n"
                + "SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] = 'V' AND category = 0 AND [name] > @name ORDER BY [name])\n"
                + "END\n";
        executeStatement(sqlStatement);
    }

}
