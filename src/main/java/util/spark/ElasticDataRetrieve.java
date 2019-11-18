package util.spark;

import dao.elsaticsearch.ElasticSearch;
import org.apache.spark.api.java.JavaPairRDD;
import util.Config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticDataRetrieve implements Serializable {
    private SparkDataProcess dataProcess = new SparkDataProcess();
    private JavaPairRDD<String, Map<String, Object>> esRdd;
    private Map<String, JavaPairRDD<String, Map<String, Object>>> esRddMap = new HashMap<>();
    private List<String> indices = Config.getElasticsearchIndices();
    private Long count = 0L;

    public JavaPairRDD<String, Map<String, Object>> retrieve (ElasticSearch es, String index, Long start, Long end) {
        if (Thread.currentThread().isInterrupted())
            return null;
        if (end <= start)
            return null;
        if (index == null)
            return null;
        return dataProcess.resetJavaPairRDD(index, es.getResult(index, start, end));
    }

    public Map<String, JavaPairRDD<String, Map<String, Object>>> retrieveAll (ElasticSearch es, Long start, Long end) {
        count = 0L;
        if (end <= start)
            return null;
        if (indices == null)
            return null;
        for (String index : indices) {
            esRdd = dataProcess.resetJavaPairRDD(index, es.getResult(index, start, end));
            if (esRdd == null)
                continue;
            esRddMap.put(index, esRdd);
            count = count + esRdd.count();
        }
        return esRddMap;
    }

    public Map<String, JavaPairRDD<String, Map<String, Object>>> retrieveAll (ElasticSearch es, Long start, Long end, Long recordMoreThan) {
        if (Thread.currentThread().isInterrupted())
            return null;
        retrieveAll(es, start, end);
        if (count <= recordMoreThan)
            return null;
        return esRddMap;
    }
}
