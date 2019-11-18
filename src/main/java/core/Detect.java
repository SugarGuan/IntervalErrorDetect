package core;

import core.detect.FieldHotKeyDetector;
import dao.elsaticsearch.ElasticSearch;
import org.apache.spark.api.java.JavaPairRDD;
import util.spark.ElasticDataRetrieve;
import util.Time;

import java.io.*;
import java.util.Map;

public class Detect implements Serializable {

    /**
     *  Detect 类是检测模式的控制器类
     *  它调用相应的检测组件FieldHotKeyDetector类对象的检测方法。
     */

    private Long detectStartTime = 0L;
    private Long detectFinishTime = 0L;
    private Long jobStartTime = 0L;
    private Long jobFinishTime = 0L;
    private ElasticSearch es;

    public Detect(ElasticSearch es) {
        this.es = es;
    }

    public void autorun () throws InterruptedException {
            jobStartTime = Time.now();
            execute();
            jobFinishTime = Time.now();
    }

    public void execute () throws InterruptedException {
        detectStartTime = getDetectStartTime();
        detectFinishTime = Time.now();

        FieldHotKeyDetector f = new FieldHotKeyDetector();

        ElasticDataRetrieve dataRetrieve = new ElasticDataRetrieve();
        Map<String, JavaPairRDD<String, Map<String, Object>>> esRddMap =
                dataRetrieve.retrieveAll(es, detectStartTime,detectFinishTime,500L);

        if (null == esRddMap) {
//            System.out.println("esrdd null");
            return;
        }
        f.detect(esRddMap, detectStartTime);
        detectFinishTime = Time.now();
        setDetectStartTime(detectFinishTime);
    }

    private Long getDetectStartTime () {
        return detectStartTime;
    }

    private void setDetectStartTime (Long detectFinishTime) {
        this.detectStartTime = detectFinishTime;
    }

}
