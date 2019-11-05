package core.learn.index;

import core.learn.field.CmdField;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.*;

public class AmsModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_ams");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> fieldString;
                    int i = 1;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        for (Map<String, Object> map: maps) {
                            fieldString = new ArrayList<>();
                            fieldString.add((String) map.get("i_cmd"));
                        }
                        CmdField.append(fieldString);
                        i++;
                        return null;
                    }
                }
        ).collect();
    }
}
