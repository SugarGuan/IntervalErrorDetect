package core;

import core.learn.FieldHotkeyFindLoader;
import dao.elsaticsearch.ElasticSearch;
import org.apache.spark.api.java.JavaPairRDD;
import util.file.ResultBackup;
import util.spark.ElasticDataRetrieve;
import util.Time;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class Learn implements Serializable {
    private Long queryStartTime = 0L;
    private Long queryFinishTime = 0L;
    private Long jobStartTime = 0L;
    private Long jobFinishTime = 0L;
    private ElasticSearch es ;
    public Learn(ElasticSearch es) {
        this.es = es;
    }
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
            jobStartTime = Time.now();
            execute();
            jobFinishTime = Time.now();
    }

    public void execute (){
        queryStartTime = getQueryStartTime();
        queryFinishTime = getQueryFinishTime();

        ElasticDataRetrieve dataRetrieve = new ElasticDataRetrieve();
        Map<String, JavaPairRDD<String, Map<String, Object>>> esRddMap =
                dataRetrieve.retrieveAll(es, queryStartTime,queryFinishTime,500L);
        if (null == esRddMap)
            return;

        FieldHotkeyFindLoader learn = new FieldHotkeyFindLoader();
        Map<String, List<List<String>>> result = learn.execute(esRddMap);
        ResultBackup file = new ResultBackup();
        file.save(result);

//        FieldHotkeyFinder fieldHotkeyFinder = new FieldHotkeyFinder();
//        fieldHotkeyFinder.learn(queryStartTime, queryFinishTime);
//        setQueryStartTime(queryFinishTime);
    }

}
