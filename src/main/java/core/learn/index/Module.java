package core.learn.index;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.Map;

public abstract class Module  implements Serializable {
    public abstract JavaPairRDD<String, Map<String, Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap);
    public abstract void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap);
}
