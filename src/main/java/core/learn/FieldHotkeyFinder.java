package core.learn;

import dao.elsaticsearch.ElasticSearch;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import util.Config;
import util.file.ResultBackup;
import util.spark.ElasticDataRetrieve;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FieldHotkeyFinder implements Serializable {

    ElasticSearch es;
    ElasticDataRetrieve elasticDataRetrieve = new ElasticDataRetrieve();
    ResultBackup resultBackup = new ResultBackup();
    List<String> indices = Config.getElasticsearchIndices();
    Map<String, List<String>> indicesFieldDict = Config.getElasticSearchIndexFieldDict();
    HotkeyFinder hotkeyFinder = new HotkeyFinder();

    JavaPairRDD<String, Map<String, Object>> indexRDD ;
    List<String> fields;
    List<List<String>> fieldOpcodeLists;
    List<String> fieldOpcodeList;

    public FieldHotkeyFinder(ElasticSearch es) {
        this.es = es;
    }

    public void learn(Long startTime, Long endTime) {
        for (String index : indices) {
            fields = indicesFieldDict.get(index);
            if (fields == null)
                continue;
            indexRDD = elasticDataRetrieve.retrieve(es, index, startTime, endTime);
            if (indexRDD == null)
                continue;
            if (indexRDD.count() == 0L)
                continue;

            for (String field : fields) {
                fieldOpcodeLists = new ArrayList<>();
                indexRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        fieldOpcodeList = new ArrayList<>();
                        for (Map<String, Object> map : maps) {
                            if (map.get(field) instanceof String) {
                                fieldOpcodeList.add((String) map.get(field));
                            }
                            if (map.get(field) instanceof Long) {
                                fieldOpcodeList.add(Long.toString((Long) map.get(field)));
                            }
                            if (map.get(field) instanceof Integer) {
                                fieldOpcodeList.add(Integer.toString((Integer) map.get(field)));
                            }
                            fieldOpcodeLists.add(fieldOpcodeList);
                        }
                        hotkeyFinder.appendOperationLists(fieldOpcodeLists);
                        resultBackup.saveFile(field, hotkeyFinder.getFrequentOperationList());
                        return null;
                    }
                }).collect();
            }

        }
    }

    private void cleanFieldOpcodeLists () {
        fieldOpcodeLists = new ArrayList<>();
    }

    private void appendFieldOpcodeLists (List<String> opCodeList) {
        fieldOpcodeLists.add(opCodeList);
    }
}
