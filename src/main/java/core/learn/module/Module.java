package core.learn.module;

import org.apache.spark.api.java.JavaPairRDD;

import java.io.Serializable;
import java.util.Map;

public abstract class Module  implements Serializable {
    public abstract JavaPairRDD<String, Map<String, Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap);
    public abstract void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap);
}
