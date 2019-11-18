import dao.elsaticsearch.ElasticSearch;
import util.websocket.Client;



public class Main {
    public static void main(String[] args) {

        /**
         *
         * Main.main()为主程序入口
         * ElasticSearch是SparkElasticSearch的方法类，该类提供操作ElasticSearch资源，包括获取、插入记录等方法；
         * Client是WebSocket接听器类，程序接收服务器发来的消息，切换当前模式。
         *
         * 由于SparkContext只允许实例化一个对象，这将导致切换进程如果重新生成ElasticSearch对象时重复实例化SparkContext
         * 所以这里不得不使用依赖注入
         *
         * websocket实例的autoReceive方法打开websocket收信功能，websocket接收到消息后直接调用切换模式的方法。
         *
         *
        **/

        ElasticSearch es = new ElasticSearch();
        Client websocketClient = new Client(es);
        websocketClient.autoReceive();

    }
}
