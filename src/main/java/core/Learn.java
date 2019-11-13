package core;

import core.learn.FieldHotkeyFindLoader;
import dao.elsaticsearch.ElasticSearch;
import org.apache.spark.api.java.JavaPairRDD;
import util.Config;
import util.ResultBackup;
import util.Spark.SparkDataProcess;
import util.Time;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Learn implements Serializable {
    private Long count = 0L;
    private Long queryStartTime = 0L;
    private Long queryFinishTime = 0L;
    private Long jobStartTime = 0L;
    private Long jobFinishTime = 0L;
    private Long round = 0L;

    private Long getQueryStartTime () {
        return queryStartTime;
    }

    private Long getQueryFinishTime () {
        return Time.now();
    }

    private void setQueryStartTime (Long queryStartTime) {
        this.queryStartTime = queryStartTime;
    }

    public void autorun () throws InterruptedException{
//        Thread.sleep();
        while (!Thread.currentThread().isInterrupted()) {
            jobStartTime = Time.now();
            count = 0L;
            execute();
            jobFinishTime = Time.now();
            System.out.println("Execute Duration Round " + round++ + ": Runtime " +Time.timeFormatEnglish(jobFinishTime - jobStartTime));
            System.out.println("-----------------------------------------------------");
            Thread.sleep(1000);
        }
    }

    public void execute (){
        ElasticSearch elasticSearch = new ElasticSearch();
        SparkDataProcess dataProcess = new SparkDataProcess();
        List<String> indices = Config.getElasticsearchIndices();
        queryStartTime = getQueryStartTime();
        queryFinishTime = getQueryFinishTime();

        System.out.println("-----------------------------------------------------");
        System.out.println("Learning mode");
        System.out.println("Data Retrieve since " + queryStartTime + " to " + queryFinishTime);

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
            return ;
        }

        setQueryStartTime(queryFinishTime);
        System.out.println("Retrieve " + count + " records.");


        System.out.println("Data Retrieve Duration : " + Time.timeFormatEnglish(Time.now() - jobStartTime));

        FieldHotkeyFindLoader learn = new FieldHotkeyFindLoader();
        Map<String, List<List<String>>> result = learn.execute(esRddMap);
        System.out.println(result.get("cmd"));
        ResultBackup file = new ResultBackup();
        file.save(result);
    }

}
