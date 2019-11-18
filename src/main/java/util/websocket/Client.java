package util.websocket;

import com.alibaba.fastjson.JSONException;
import core.Switch;
import dao.elsaticsearch.ElasticSearch;
import dao.websocket.Model;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.drafts.Draft_6455;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.net.URISyntaxException;

import com.alibaba.fastjson.JSON;
import util.Config;

public class Client {

    /**
     * Client 类
     * 用于切换plugin模式的核心类，负责：
     * 1.初试化启动检测模式（Detecting mode）
     * 2.接收websocket发来的模式切换信息
     * 3.维护一个websocket常开的线程
     * 4.解析websocket发来的json消息，不满足格式的消息（干扰消息）将被丢弃
     * 5.提供一个向switch发送调用命令的函数
     */

    private boolean initialFlag = false;
    private Switch switcher;
    private static WebSocketClient client;
    private static StringBuffer uri = new StringBuffer("ws://");

    /**
     * Client()
     * 构造方法传入es实例依赖
     * @param elasticSearch es实例
     */
    public Client (ElasticSearch elasticSearch) {
        switcher = new Switch(elasticSearch);
    }

    /**
     * autoReceive()
     * 构造一个websocket对象，维持websocket连接
     * 解析收到的信息，并通知模式切换
     */
    public void autoReceive() {
        String ipAddr = Config.getWebSocketIP();
        int port = Config.getWebSocketPort();
        if (!initialFlag) {
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
                    noticeModeChange(str);

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

    /**
     * noticeModeChange()
     * 通知模式切换方法：
     * 1.解析收到的string，查看是否是满足json model的json格式（不满足抛出JSONException异常，但不做处理，仅仅是短路掉模式切换）
     * 2.通知模式切换
     * @param str 接收到的websocket信息（String）
     */
    public void noticeModeChange(String str) {
        //message.setLength(0);
        // Try to catch the class cast un excepted error.
        try{
            Model model = JSON.parseObject(str, Model.class);
            switcher.autoSwitch(model.getState());
            System.out.println(str); // 该方法用于验证收到的消息内容，是检测用。具体应用中需要关闭！
        } catch (JSONException e){
            // Unexpected input, doing nothing.
        }

        //message.append(model.getState());
    }

}
