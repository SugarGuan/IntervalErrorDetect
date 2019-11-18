package util.spark;

import dao.elsaticsearch.ElasticSearch;
import org.apache.spark.api.java.JavaPairRDD;
import util.Config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticDataRetrieve implements Serializable {

    /**
     *  ElasticDataRetrieve 类
     *  该类将ES操作中的数据提取封装，提供了某种条件下从es中获取单个index和获取全部index的各项方法。
     */

    private SparkDataProcess dataProcess = new SparkDataProcess();
    private JavaPairRDD<String, Map<String, Object>> esRdd;
    private Map<String, JavaPairRDD<String, Map<String, Object>>> esRddMap = new HashMap<>();
    private List<String> indices = Config.getElasticsearchIndices();
    private Long count = 0L;

    /**
     * retrieve()
     * 获取一个index 的数据rdd
     * 由于数据拉取数据速度较慢且满足原子性，在操作之前检测当前进程是否挂起，如果挂起则立刻返回，节省时间；
     * 尽快结束函数调用，返回调用函数模块执行线程终止逻辑。
     * @param es elasticsearch实例
     * @param index 索引名
     * @param start 起始时间戳
     * @param end 结束时间戳
     * @return 查询结果的pairRDD（可以为空）
     */
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
