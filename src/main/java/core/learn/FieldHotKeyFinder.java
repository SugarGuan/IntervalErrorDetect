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

public class FieldHotKeyFinder implements Serializable {
    /**
     *  FieldHotKeyFinder 类
     *  该类是新版学习模式的核心类 （暂未启用）
     *  它从RDD中获取对应的队列，统计序列出现频次，存入规则文件（以对应field命名）
     */
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

    /**
     *
     * FieldHotKeyFinder()  （暂未启用）
     * 初始化时注入依赖 es实例
     * @param es
     */
    public FieldHotKeyFinder(ElasticSearch es) {
        this.es = es;
    }

    /**
     * learn()
     * 学习模式主逻辑  （暂未启用）
     * @param startTime 本轮学习的起始时间
     * @param endTime 本轮学习的终止时间
     */
    public void learn(Long startTime, Long endTime) {
        for (String index : indices) {
            fields = indicesFieldDict.get(index);
            if (fields == null)
                continue;
            indexRDD = elasticDataRetrieve.retrieve(es, index, startTime, endTime);
            if (Thread.currentThread().isInterrupted())
                return ;
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
