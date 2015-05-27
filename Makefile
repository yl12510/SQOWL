CLASSPATH := .:lib/msbase.jar:lib/mssqlserver.jar:lib/msutil.jar
PIG_CLASSPATH := .:/Users/yl12510/Desktop/pig-0.12.0-cdh5.3.2-withouthadoop.jar
#DEBUG := 0
#DBUSER := yl12510
#DBPASSWD := 6tszYXAaEt
#DBDRIVER := org.postgresql.Driver
#DBURL := jdbc:postgresql://db.doc.ic.ac.uk/

dbNoInf : src/main/doc/ic/ac/uk/sqowl/rdbms/dbNoInf.class
	java -cp ${CLASSPATH} src.main.doc.ic.ac.uk.sqowl.rdbms.dbNoInf
#-ontology lubm \
#-debug ${DEBUG} \
#-user ${DBUSER} \
#-password ${DBPASSWD} \
#-driver ${DBDRIVER} \
#-url ${DBURL}yl12510 \
#-nofks \
#-local_names \
#-print

src/main/doc/ic/ac/uk/sqowl/rdbms/dbNoInf.class : src/main/doc/ic/ac/uk/sqowl/rdbms/dbNoInf.java
	javac -cp ${CLASSPATH} src/main/doc/ic/ac/uk/sqowl/rdbms/dbNoInf.java

pig : src/exercise/pig/ConnectToPig.class
	java -cp ${PIG_CLASSPATH} src.exercise.pig.ConnectToPig

src/exercise/pig/ConnectToPig.class : src/exercise/pig/ConnectToPig.java
	javac -cp ${PIG_CLASSPATH} src/exercise/pig/ConnectToPig.java

