package core;

import dao.elsaticsearch.ElasticSearch;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
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
    List<String> lib = new ArrayList<>();
    List<String> commandLists;
    int cmd = 0;


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

        File f = new File("D:\\Project\\2020\\dig-lib\\cmd.iedb");
        try {
            BufferedReader br = new BufferedReader(new FileReader(f));
            String strTemp;
            while(null != (strTemp = br.readLine())) {
                lib.add(strTemp);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        ElasticDataRetrieve dataRetrieve = new ElasticDataRetrieve();

        Map<String, JavaPairRDD<String, Map<String, Object>>> esRddMap =
                 dataRetrieve.retrieve(detectStartTime, detectFinishTime);
        JavaPairRDD<String, Map<String, Object>> rdd = esRddMap.get("au_pkt_ams");
        if (rdd == null)
            return ;
        if (rdd.count() == 0)
            return ;

        System.out.println(rdd.count());

        rdd.map(new Function<Tuple2<String, Map<String, Object>>, Object>() {
            @Override
            public Object call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
                Map<String, Object> map = stringMapTuple2._2;
                commandLists = new ArrayList<>();
//                String startTime = (String) map.get("@timestamp");
//                if(commandLists.size()==20)
//                    commandLists.remove(0);
//                commandLists.add( map.get("i_cmd"));
//                if (checkInfile(commandLists)==true){
//                    alert();
//                } else {
//                    System.out.println("None");
//                }
                System.out.println(map.get("i_cmd"));
                return null;
            }
        }).collect();
        detectFinishTime = Time.now();
        setDetectStartTime(detectFinishTime);
    }

    private Long getDetectStartTime () {
        return detectStartTime;
    }

    private void setDetectStartTime (Long detectFinishTime) {
        this.detectStartTime = detectFinishTime;
    }

    private boolean checkInfile(List<String> list) {
        int listLength = list.size();
        int i = 0;
        if (listLength == 0)
            return false;
        StringBuffer sb = new StringBuffer();
        List<String> subList;
        while (i < listLength) {
            subList = list.subList(i, listLength);
            for (String str : subList) {
                sb.append(str);
                sb.append(",");
            }

            String s = sb.toString();
            System.out.println(s);
            if (lib.contains(s))
                return true;
            i++;
        }
        return false;
    }

    private void alert() {
        System.out.println("Founded.");
    }
}
