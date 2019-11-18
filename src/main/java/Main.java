import core.Clean;
import core.Detect;
import core.Learn;
import core.Switch;
import dao.elsaticsearch.ElasticSearch;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.java_websocket.client.WebSocketClient;
import util.Config;
import util.Time;
import util.websocket.Client;

import java.net.URI;
import java.net.URISyntaxException;

public class Main {
    public static int count = 0;
    public static void main(String[] args) {
        Long start = Time.now();

        ElasticSearch es = new ElasticSearch();
        Client websocketClient = new Client(es);
        websocketClient.autoReceive();
        Long end = Time.now();
//
//        try{
//            Thread t = new Thread(learn2);
//            t.start();
//            Thread.sleep(50* 60 * 1000);
//            System.out.println("Mode Interrupted : \"detecting Mode.\"");
//            t.interrupt();
//        } catch (InterruptedException e) {
//
//        }



    }
}
