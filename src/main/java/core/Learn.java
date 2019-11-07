package core;

import core.learn.field.CmdField;
import core.learn.module.AmsModule;
import org.apache.spark.api.java.JavaPairRDD;

import dao.elsaticsearch.ElasticSearch;
import dao.file.Config;
import util.Spark.SparkDataProcess;
import util.Spark.ml.FPGrowthCal;
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
//            execute();
//        }
    }

    public void execute (){
        jobStartTime = Time.now();

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

        if (count <= 500)
            return ;

        setQueryStartTime(queryFinishTime);
        System.out.println(count);

        AmsModule ams = new AmsModule();
        ams.fieldFillin(esRddMap);

        FPGrowthCal.execute(CmdField.getRDD().distinct());


        jobFinishTime = Time.now();
        System.out.println("Duration :" + Time.timeFormatEnglish(jobFinishTime - jobStartTime));
    }

}
