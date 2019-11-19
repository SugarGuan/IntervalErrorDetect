package core;

import core.detect.Alerter;
import core.detect.FieldHotKeyDetector;
import core.detect.FieldHotKeyDetectorV2;
import core.learn.FieldHotkeyFindLoader;
import dao.elsaticsearch.ElasticSearch;
import dao.redis.Redis;
import org.apache.spark.api.java.JavaPairRDD;
import util.Config;
import util.spark.ElasticDataRetrieve;
import util.Time;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class Detect implements Serializable {

    /**
     *  Detect 类
     *  检测模式的控制器类
     *  它调用相应的检测组件FieldHotKeyDetector类对象的检测方法。
     */

    private Long detectStartTime = 0L;
    private Long detectFinishTime = 0L;
    private Long jobStartTime = 0L;
    private Long jobFinishTime = 0L;
    private ElasticSearch es;
    private Redis redis;

    public Detect(ElasticSearch es) {
        this.es = es;
    }

    public void autorun () throws InterruptedException {
        redis = new Redis();
        while(!Thread.currentThread().isInterrupted()) {
            jobStartTime = Time.now();
            execute();
            jobFinishTime = Time.now();
            // 单位为毫秒，1s = 1000ms
            System.out.println("[INFO] Detecting mode successfully finish her homework in " +
                                    Time.timeFormatEnglish(jobFinishTime - jobStartTime) + ".");
            Thread.sleep(Config.getSleepTime() * 1000);
        }

    }

    public void execute () throws InterruptedException {
        detectStartTime = getDetectStartTime();
        detectFinishTime = Time.now();

//        FieldHotKeyDetector f = new FieldHotKeyDetector();

        ElasticDataRetrieve dataRetrieve = new ElasticDataRetrieve();

        if (Thread.currentThread().isInterrupted())
            return ;
        Map<String, JavaPairRDD<String, Map<String, Object>>> esRddMap =
                dataRetrieve.retrieveAll(es, detectStartTime,detectFinishTime,500L);

        if (null == esRddMap) {
//            System.out.println("esrdd null");
            return;
        }
        if (Thread.currentThread().isInterrupted())
            return ;
//        f.detect(esRddMap, detectStartTime, redis);
        FieldHotKeyDetectorV2 learn = new FieldHotKeyDetectorV2();
        Map<String, List< Map<List<String>, Long> > > result = learn.execute(esRddMap);
        if (Thread.currentThread().isInterrupted())
            return ;
        Alerter alerter = new Alerter();
        alerter.superReport(result, detectStartTime, redis);
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
