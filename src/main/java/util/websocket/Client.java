package util.websocket;

import com.alibaba.fastjson.JSONException;
import core.Switch;
import dao.websocket.Model;
import org.java_websocket.WebSocket;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.drafts.Draft_6455;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.net.URISyntaxException;

import com.alibaba.fastjson.JSON;

public class Client {
    private boolean initialFlag = false;
    private Switch switcher = new Switch();
    private static WebSocketClient client;
    private static StringBuffer uri = new StringBuffer("ws://");

    public void autoReceive(String ipAddr, int port) {
        if (initialFlag == false) {
            // 默认模式下进入检测模式 (study: off)
            switcher.autoSwitch("off");
            initialFlag = true;
        }

        try{
            uri.append(ipAddr).append(":").append(Integer.toString(port));
            client = new WebSocketClient(new URI(uri.toString()), new Draft_6455()) {
                @Override
                public void onOpen(ServerHandshake serverHandshake) {
                    System.out.println("Socket connection established.");
                }

                @Override
                public void onMessage(String str) {
                    setMessage(str);

                }

                @Override
                public void onClose(int i, String s, boolean b) {
                    System.out.println("Socket connection closed.");
                }

                @Override
                public void onError(Exception e){
                    e.printStackTrace();
                }
            };

        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        client.connect();

        System.out.println(client.getDraft());

//        while(!client.getReadyState().equals(WebSocket.READYSTATE.OPEN)){
//            // Do nothing but wait.
//        }
        // System.out.println("Socket Connection open.");
    }

    public void setMessage(String str) {
        //message.setLength(0);
        // Try to catch the class cast un excepted error.
        try{
            Model model = JSON.parseObject(str, Model.class);
            switcher.autoSwitch(model.getState());
            System.out.println(str);
        } catch (JSONException e){
            // Unexpected input, doing nothing.
        }

        //message.append(model.getState());
    }

}
