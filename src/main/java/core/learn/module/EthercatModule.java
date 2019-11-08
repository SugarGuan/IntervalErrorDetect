package core.learn.module;

import core.learn.field.CmdField;
import core.learn.field.Cmd_strField;
import core.learn.field.Offset_addrField;
import core.learn.field.Slave_addrField;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class EthercatModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_ethercat");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap)  {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> cmdField;
                    List<String> cmdstrField;
                    List<String> slave_addrField;
                    List<String> offset_addrField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        cmdField = new ArrayList<>();
                        cmdstrField = new ArrayList<>();
                        slave_addrField = new ArrayList<>();
                        offset_addrField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            String cmd = Long.toString((Long) map.get("i_cmd"));
                            String cmd_str = (String) map.get("i_cmd_str");
                            String slave_addr = Long.toString((Long) map.get("i_slave_addr"));
                            String offset_addr = Long.toString((Long) map.get("i_offset_addr"));

                            if(cmdField.contains(cmd)) {
                                CmdField.append(cmdField);
                                cmdField = new ArrayList<>();
                            }

                            if(cmdstrField.contains(cmd_str)) {
                                Cmd_strField.append(cmdstrField);
                                cmdstrField = new ArrayList<>();
                            }

                            if(slave_addrField.contains(slave_addr)) {
                                CmdField.append(slave_addrField);
                                slave_addrField = new ArrayList<>();
                            }

                            if(offset_addrField.contains(offset_addr)) {
                                Offset_addrField.append(offset_addrField);
                                offset_addrField = new ArrayList<>();
                            }

                            cmdField.add(cmd);
                            cmdstrField.add(cmd_str);
                            slave_addrField.add(slave_addr);
                            offset_addrField.add(offset_addr);
                        }
                        CmdField.append(cmdField);
                        Cmd_strField.append(cmdstrField);
                        Slave_addrField.append(slave_addrField);
                        Offset_addrField.append(offset_addrField);
                        return null;
                    }
                }
        ).collect();
    }
}
