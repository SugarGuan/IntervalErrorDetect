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
     * 初始化时注入依赖 es 实例
     * @param es 注入的 es 实例依赖
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
        /**
         *   从ElasticSearch获取数据的速度可能很慢，所以获取ES的策略有2种：
         *   1. 一次性全局拉取数据
         *   2. 每次拉取一个index的数据。整体数据分多次拉取
         *
         *   当采用全局拉取数据的时候，首次拉取速度很慢，4800万条数据拉取时长约为11分钟（含网络传输）
         *   但后续每次拉取数据可能无法达到此值。
         *   单次拉取的时长太长，可能导致无法及时终止模式的线程切换。
         *
         *   一旦采用每次拉取一个index的数据可能引发多次拉取，速度无法估计。
         *   但是可以保证每个操作的粒度较细，每个数据拉取过程不过长，可以以较快速度完成模式切换。
         */
        for (String index : indices) {

            /**
             *  从索引字段对照表中获取当前索引下应当分析的字段列表
             */

            fields = indicesFieldDict.get(index);
            if (fields == null)
                continue;

            /**
             *  由于拉取数据过程可能很慢
             *  考虑到尽可能及时结束当前线程任务，响应模式切换指令：
             *  在数据拉取前后均判断当前进程是否被外置为中断模式
             */

            if (Thread.currentThread().isInterrupted())
                return ;

            indexRDD = elasticDataRetrieve.retrieve(es, index, startTime, endTime); // 拉取当前索引字段

            if (Thread.currentThread().isInterrupted())
                return ;

            if (indexRDD == null)
                continue;

            if (indexRDD.count() == 0L)
                continue;

            /**
             *  对于列表中待分析字段展开分析：每次只根据一个字段组成操作码队列
             */

            for (String field : fields) {
                System.out.println("[INFO] LEARNING MODE : " + "INDEX '" + index + "' FIELD '" + field +" '");
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
