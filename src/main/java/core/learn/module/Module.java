package core.learn.module;

import org.apache.spark.api.java.JavaPairRDD;

import java.io.Serializable;
import java.util.Map;

public abstract class Module  implements Serializable {

    /**
     * Module 类
     * Module类是所有索引类的父类。
     * 索引类提供取回从RDD集合中取回当前集合RDD、向字段实体填充内容（用于分析）的功能。
     */

    /**
     * retrieveRDD() abstract
     * 从集合中取回当前索引的RDD
     * @param rddMap RDD集合
     * @return 当前索引的RDD
     */
    public abstract JavaPairRDD<String, Map<String, Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap);

    /**
     * fieldFillin() abstract
     * 向字段分析类插入操作队列，以供其展开统计分析
     * @param rddMap RDD的集合（需要耦合调用retrieveRDD()）
     */
    public abstract void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap);
}
