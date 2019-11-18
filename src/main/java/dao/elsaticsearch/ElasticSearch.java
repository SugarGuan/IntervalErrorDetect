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
    private String elasticsearchAddr;
    private String elasticsearchPort;
    private String elasticsearchUsername;
    private String elasticsearchPassword;
    private String elasticsearchIndexAutoCreate = "true";               // Sets true, the index auto generate opened.
    private String elasticsearchIndexReadMissingAsEmpty = " true";
    private boolean esparkConfUpdated = false;

    public ElasticSearch() {
        elasticsearchAddr = "10.245.142.213";
        elasticsearchPort = "9200";
    }

    public ElasticSearch(String elasticsearchAddr, int elasticsearchPort) {
        this.elasticsearchAddr = elasticsearchAddr;
        this.elasticsearchPort = Integer.toString(elasticsearchPort);
    }

    public ElasticSearch(String elasticsearchAddr, int elasticsearchPort, String elasticsearchUsername, String elasticsearchPassword){
        this.elasticsearchAddr = elasticsearchAddr;
        this.elasticsearchPort = Integer.toString(elasticsearchPort);
        this.elasticsearchUsername = elasticsearchUsername;
        this.elasticsearchPassword = elasticsearchPassword;
    }

    private List<String> getElasticsearchIndices() {
        return Config.getElasticsearchIndices();
    }

    public SparkConf getESparkConf(){
        if (!esparkConfUpdated)
            updateEsparkConf();
        return Spark.getSparkConf();   // updated
    }

    private void updateEsparkConf(){
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

    public JavaSparkContext getESparkContext(){
        updateEsparkConf();
        JavaSparkContext sc = Spark.getSparkContext();
        return sc;
    }

    public JavaPairRDD<String, Map<String, Object>> getResult(){
        return getResult(0L, Time.now());
    }

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
