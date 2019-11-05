package core.learn.field;

import org.apache.spark.api.java.JavaRDD;

import java.util.List;

public interface Field {

    static void append (List<String> strList) {};

    static List<List<String>> getStrList() { return null; };

    static JavaRDD<List<String>> getRDD() { return null; };

}
