package core;

import core.learn.field.CmdField;
import core.learn.index.AmsModule;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;

import dao.elsaticsearch.ElasticSearch;
import dao.file.Config;
import util.Spark.SparkDataProcess;
import util.Spark.ml.FPGrowthCal;
import util.Time;

import java.io.Serializable;
import java.util.*;

public class Learn implements Serializable {
    public static int count = 0;
    public void execute(){
        ElasticSearch elasticSearch = new ElasticSearch();
        SparkDataProcess dataProcess = new SparkDataProcess();
        List<String> indices = Config.getElasticsearchIndices();
//        Config config = new Config();
//        Long startTime = Time.getLastTime();
//        Long endTime = Time.getRealTime();

        Long a, b = 0L;

        Long startTime = 1570777465000L;
        Long endTime = 1572942236007L;
        Long count = 0L;

        a = Time.now();

        JavaPairRDD<String, Map<String, Object>> esRdd;
        Map<String, JavaPairRDD<String, Map<String, Object>>> esRddMap = new HashMap<>();
        for (String index : indices) {
            esRdd = dataProcess.resetJavaPairRDD(index, elasticSearch.getResult(index, startTime, endTime));
            if (esRdd == null)
                continue;
            esRddMap.put(index, esRdd);
            count = count + esRdd.count();
        }

        if (count <= 500)
            return ;

        Time.SetLastTime(endTime);
        System.out.println(count);

        AmsModule ams = new AmsModule();

        ams.fieldFillin(esRddMap);

        FPGrowthCal.execute(CmdField.getRDD());

        b = Time.now();
        System.out.println("Duration :" + (b - a)/1000.0 + " s");



    }
}
