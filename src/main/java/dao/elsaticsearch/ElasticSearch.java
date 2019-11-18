package dao.elsaticsearch;

import util.spark.Spark;
import util.Config;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import util.Time;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class ElasticSearch implements Serializable {
    /**
     *  ElasticSearch 是操作ElasticSearch的类
     *  本Plugin使用ESpark对ES数据进行操作，所以本类提供：ESpark 场景SparkConf生成、获取、ES数据查询等功能。
     */


    private String elasticsearchAddr;
    private String elasticsearchPort;
    private String elasticsearchUsername;
    private String elasticsearchPassword;
    private String elasticsearchIndexAutoCreate = "true";               // Sets true, the index auto generate opened.
    private String elasticsearchIndexReadMissingAsEmpty = " true";
    private boolean esparkConfUpdated = false;

    /**
     * 默认的构造函数，确定ES地址和端口号
     */
    public ElasticSearch() {
        elasticsearchAddr = "10.245.142.213";
        elasticsearchPort = "9200";
    }

    /**
     * 根据提供的IP地址和端口号生成ES操作对象
     * @param elasticsearchAddr ES地址
     * @param elasticsearchPort ES端口
     */
    public ElasticSearch(String elasticsearchAddr, int elasticsearchPort) {
        this.elasticsearchAddr = elasticsearchAddr;
        this.elasticsearchPort = Integer.toString(elasticsearchPort);
    }

    /**
     * 根据提供的IP地址、端口号、ES用户名和密码生成ES操作对象
     * @param elasticsearchAddr ES地址
     * @param elasticsearchPort ES端口
     * @param elasticsearchUsername ES用户名
     * @param elasticsearchPassword ES密码
     */
    public ElasticSearch(String elasticsearchAddr, int elasticsearchPort, String elasticsearchUsername, String elasticsearchPassword){
        this.elasticsearchAddr = elasticsearchAddr;
        this.elasticsearchPort = Integer.toString(elasticsearchPort);
        this.elasticsearchUsername = elasticsearchUsername;
        this.elasticsearchPassword = elasticsearchPassword;
    }

    /**
     * 获取ES的索引信息
     * @return ES的索引信息列表（String）
     */
    private List<String> getElasticsearchIndices() {
        return Config.getElasticsearchIndices();
    }

    /**
     * SparkConf()
     * 获取更新后的SparkConf。如果SparkConf没更新，则更新该SporkConf后返回
     * @return SparkConf实例
     */
    public SparkConf SparkConf(){
        if (!esparkConfUpdated)
            updateEsparkConf();
        return Spark.getSparkConf();   // updated
    }

    /**
     * updateEsparkConf()
     * 更新SparkConf，向内注入ESpark配置项。如果已经更新过则不再更新.
     */
    private void updateEsparkConf(){

        /**
         * es.index.auto.create => 自动创建索引
         * es.nodes => 自动创建节点
         * es.port => es 的端口号
         * es.index.read.missing.as.empty => es index 缺失时视作空值（查询时如果不存在该index返回空值，不报错）
         * es.net.http.auth.user => elastic search 用户名
         * es.net.http.auth.pass => elastic search 密码
         */

        if (esparkConfUpdated)
            return ;
        Spark.appendSparkConf("es.index.auto.create", elasticsearchIndexAutoCreate);
        Spark.appendSparkConf("es.nodes", elasticsearchAddr);
        Spark.appendSparkConf("es.port", elasticsearchPort);
        Spark.appendSparkConf("es.index.read.missing.as.empty", elasticsearchIndexReadMissingAsEmpty);
        if (null != elasticsearchUsername)
            Spark.appendSparkConf("es.net.http.auth.user", elasticsearchUsername);
        if (null != elasticsearchPassword)
            Spark.appendSparkConf("es.net.http.auth.pass", elasticsearchPassword);
        esparkConfUpdated = true;
    }

    /**
     * getESparkContext()
     * 获取设定好Elastic Search设置项的Spark Context对象
     * @return SparkContext对象， 单例
     */
    public JavaSparkContext getESparkContext(){
        updateEsparkConf();
        JavaSparkContext sc = Spark.getSparkContext();
        return sc;
    }

    /**
     * getResult()
     * 缺省过滤项的RDD结果查询函数，设置起始时间戳为0L ~ 终止时间戳 当前时间 的查询结果（全量）
     * @return JavaPairRDD
     */
    public JavaPairRDD<String, Map<String, Object>> getResult(){
        return getResult(0L, Time.now());
    }

    /**
     * getResult()
     * 根据提供的时间范围获取ES查询结果
     * @param startMillTime 起始时间戳
     * @param endMillTime 结束时间戳
     * @return Elastic Search 查询结果
     */
    public JavaPairRDD<String, Map<String, Object>> getResult(Long startMillTime, Long endMillTime) {
        JavaSparkContext sc = getESparkContext();
        String timeRange =
                "{\"range\":{\"@timestamp\":{\"gte\":"
                + startMillTime
                + ",\"lt\":"
                + endMillTime
                + "}}}";
        JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(sc, "au_pkt_ams/au_pkt_ams", timeRange);
        for (String str: getElasticsearchIndices()) {
            esRDD.union((JavaPairRDD<String, Map<String, Object>>) JavaEsSpark.esRDD(sc, str + "/" + str, timeRange));
        }
        return esRDD;
    }

    /**
     * getResult()
     * 根据提供的索引名、起始时间、终止时间获取时间范围内的es数据
     * @param index 索引名
     * @param startMillTime 起始时间
     * @param endMillTime 结束时间
     * @return 查询到的结果RDD
     */
    public JavaPairRDD<String, Map<String, Object>> getResult(String index, Long startMillTime, Long endMillTime) {
        JavaSparkContext sc = getESparkContext();
        String timeRange =
                "{\"range\":{\"@timestamp\":{\"gte\":"
                        + startMillTime
                        + ",\"lt\":"
                        + endMillTime
                        + "}}}";
        JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(sc, index + "/" + index, timeRange);
        return esRDD;
    }

}
