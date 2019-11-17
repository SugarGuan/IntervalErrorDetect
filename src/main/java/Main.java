import core.Clean;
import core.Detect;
import core.Learn;
import core.Switch;
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
        String webSocketIPAddr = Config.getWebSocketIP();
        int port = Config.getWebSocketPort();
        Client websocketClient = new Client();
        websocketClient.autoReceive(webSocketIPAddr, port);
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
