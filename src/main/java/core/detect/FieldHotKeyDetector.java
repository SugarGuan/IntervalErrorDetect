package core.detect;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import util.Config;
import util.Time;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
public class FieldHotKeyDetector implements Serializable {
    Map<String, List<String>> indexField = Config.getElasticSearchIndexFieldDict();
    JavaPairRDD<String, Map<String, Object>> rdd;
    List<String> elasticIndices = Config.getElasticsearchIndices();
    List<String> fields;
    List<String> commandLists;
    Alerter alert;
    List<String> lib;

    public void detect(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap, Long detectStartTime) {
        alertSetter();
        for (String str : elasticIndices) {
            fields = indexField.get(str);
            if (fields == null)
                continue;
            rdd = rddMap.get(str);
            if (rdd == null)
                continue ;
            if (rdd.count() == 0)
                continue ;
            for (String field : fields) {
                libSetter(field);
                rdd.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        commandLists = new ArrayList<>();
                        for (Map<String, Object> map : maps) {

                            if (commandLists.size() >=20)
                                commandLists.remove(0);

                            if (map.get(field) instanceof Long)
                                commandLists.add(Long.toString((Long) map.get(field)));
                            else if (map.get(field) instanceof String)
                                commandLists.add((String) map.get(field));
                            else
                                continue;

                            if (checkInfile(commandLists)) {
                                alert.report(
                                        str,
                                        map,
                                        detectStartTime,
                                        Time.now()
                                );
                            }
                        }
                        return null;
                    }
                }).collect();
            }
        }
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
            sb.setLength(0);
            if (lib.contains(s))
                return true;
            i++;
        }
        return false;
    }

    private void alertSetter () {
        alert = new Alerter();
    }

    synchronized private void libSetter (String field) {
        lib = new ArrayList<>();
        if (field.length() < 3)
            return;
        try {
            File file = new File("D:\\Project\\2020\\dig-lib\\" + field.substring(2) + ".iedb");

            BufferedReader br = new BufferedReader(new FileReader(file));
            String strTemp;
            while(null != (strTemp = br.readLine())) {
                lib.add(strTemp);
            }
        } catch (Exception e) {

        }
    }
}
