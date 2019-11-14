package core.detect;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import util.Config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
public class FieldHotKeyDetector {
    Map<String, List<String>> indexField = Config.getElasticSearchIndexFieldDict();
    JavaPairRDD<String, Map<String, Object>> rdd;
    List<String> elasticIndices = Config.getElasticsearchIndices();
    List<String> fields;
    List<String> commandLists;
    Alerter alert;
    List<String> lib;

    public void detect (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        alertSetter();
        libSetter();
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
                rdd.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        commandLists = new ArrayList<>();
                        for (Map<String, Object> map : maps) {
                            if (commandLists.size() >=20)
                                commandLists.remove(0);
                            commandLists.add((String) map.get(field));
                            if (checkInfile(commandLists) == true) {
                                alert.report((String) map.get("@timestamp"), (String) map.get("i_dname"));
                            } else
                                System.out.println("None error");
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
            System.out.println(s);
            if (lib.contains(s))
                return true;
            i++;
        }
        return false;
    }

    private void alertSetter () {
        alert = new Alerter();
    }

    synchronized private void libSetter () {
        File f = new File("D:\\Project\\2020\\dig-lib\\cmd.iedb");
        lib = new ArrayList<>();
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
    }
}
