package core;

import core.learn.FieldLearn;
import org.apache.spark.api.java.JavaPairRDD;

import dao.elsaticsearch.ElasticSearch;
import scala.Tuple2;
import util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Spark.SparkDataProcess;
import util.Time;

import java.io.Serializable;
import java.util.*;

public class Learn implements Serializable {
    private Long count = 0L;
    private Long queryStartTime = 0L;
    private Long queryFinishTime = 0L;
    private Long jobStartTime = 0L;
    private Long jobFinishTime = 0L;


    private Long getQueryStartTime () {
        return queryStartTime;
    }

    private Long getQueryFinishTime () {
        return Time.now();
    }

    private void setQueryStartTime (Long queryStartTime) {
        this.queryStartTime = queryStartTime;
    }

    public void autorun () {
//        for (int i = 0; i < 100; i++) {
////            execute();
////        }
    }

    public void execute (){
        jobStartTime = Time.now();
        Logger logger = LoggerFactory.getLogger(core.Learn.class);
        ElasticSearch elasticSearch = new ElasticSearch();
        SparkDataProcess dataProcess = new SparkDataProcess();
        List<String> indices = Config.getElasticsearchIndices();

        queryStartTime = getQueryStartTime();
        queryFinishTime = getQueryFinishTime();

        JavaPairRDD<String, Map<String, Object>> esRdd;
        Map<String, JavaPairRDD<String, Map<String, Object>>> esRddMap = new HashMap<>();

        for (String index : indices) {
            esRdd = dataProcess.resetJavaPairRDD(index, elasticSearch.getResult(index, queryStartTime, queryFinishTime));
            if (esRdd == null)
                continue;
            esRddMap.put(index, esRdd);
            count = count + esRdd.count();
        }

        if (count <= 500){
            logger.info("Retrieve records less than 500. Too less records makes system analysis unreliable.");
            return ;
        }

        setQueryStartTime(queryFinishTime);
        System.out.println("Retrieve " + count + " records.");

        jobFinishTime = Time.now();
        System.out.println("Data Retrieve Duration : " + Time.timeFormatEnglish(jobFinishTime - jobStartTime));

        FieldLearn learn = new FieldLearn();
        Map<String, List<List<String>>> result = learn.execute(esRddMap);
        System.out.println(result.get("cmd"));
        jobFinishTime = Time.now();
        logger.warn("Execute Duration : " + Time.timeFormatEnglish(jobFinishTime - jobStartTime));
        System.out.println("Execute Duration (overall) : " + Time.timeFormatEnglish(jobFinishTime - jobStartTime));
    }

}
