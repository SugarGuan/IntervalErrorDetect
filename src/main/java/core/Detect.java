package core;

import core.detect.Alerter;
import core.detect.FieldHotKeyDetector;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import util.Config;
import util.Spark.ElasticDataRetrieve;
import util.Spark.SparkDataProcess;
import util.Time;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Detect implements Serializable {
    private Long detectStartTime = 0L;
    private Long detectFinishTime = 0L;
    private Long jobStartTime = 0L;
    private Long jobFinishTime = 0L;


    public void autorun () throws InterruptedException {
        while (!Thread.currentThread().isInterrupted()) {
            jobStartTime = Time.now();
            execute();
            jobFinishTime = Time.now();
            System.out.println(Time.timeFormatEnglish(jobFinishTime - jobStartTime));
            Thread.sleep(1000);
        }
    }

    public void execute () throws InterruptedException {
        System.out.println("Detected Mode");
        detectStartTime = getDetectStartTime();
        detectFinishTime = Time.now();

        FieldHotKeyDetector f = new FieldHotKeyDetector();

        ElasticDataRetrieve dataRetrieve = new ElasticDataRetrieve();
        Map<String, JavaPairRDD<String, Map<String, Object>>> esRddMap =
                dataRetrieve.retrieveAll(detectStartTime,detectFinishTime,500L);

        if (null == esRddMap) {
            System.out.println("ess null");
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
