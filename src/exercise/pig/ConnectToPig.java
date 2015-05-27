import java.io.IOException;
import org.apache.pig.PigServer;

public class ConnectToPig {

    public static void main(String[] args) {
        try {
            PigServer pigServer = new PigServer("local");
            System.out.println("yay");
            runIdQuery(pigServer, "passwd");
            System.out.println("finished!");
        } catch (Exception e) {
	    System.out.println(e);
	}
    }
    
    public static void runIdQuery(PigServer pigServer, String inputFile) throws IOException {
	pigServer.registerQuery("A = load '" + inputFile + "' using PigStorage(':');");
	pigServer.registerQuery("B = foreach A generate $0 as id;");
	pigServer.store("B", "id.out");
    }
}
