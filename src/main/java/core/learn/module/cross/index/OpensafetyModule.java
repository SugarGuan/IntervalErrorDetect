package core.learn.module.cross.index;

import core.learn.field.*;
import core.learn.module.Module;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import util.StringUtil;

import java.util.*;

public class OpensafetyModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_opensafety");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> data_typeField;
                    List<String> sender_idField;
                    List<String> data_ptField;
                    List<String> msgtypeField;
                    List<String> sn_toField;
                    List<String> sn_fromField;
                    List<String> safezoneField;
                    List<String> dirField;
                    List<String> masterField;
                    List<String> slaveField;
                    List<String> ext_ser_idField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        data_typeField = new ArrayList<>();
                        sender_idField = new ArrayList<>();
                        data_ptField = new ArrayList<>();
                        msgtypeField = new ArrayList<>();
                        sn_toField = new ArrayList<>();
                        sn_fromField = new ArrayList<>();
                        safezoneField = new ArrayList<>();
                        dirField = new ArrayList<>();
                        masterField = new ArrayList<>();
                        slaveField = new ArrayList<>();
                        ext_ser_idField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            data_typeField.add(StringUtil.trans(map.get("i_data_type")));
                            sender_idField.add(StringUtil.trans(map.get("i_sender_id")));
                            data_ptField.add(StringUtil.trans(map.get("i_data_pt_id")));
                            msgtypeField.add(StringUtil.trans(map.get("i_msgtype")));
                            sn_toField.add(StringUtil.trans(map.get("i_sn_to")));
                            sn_fromField.add(StringUtil.trans(map.get("i_sn_from")));
                            safezoneField.add(StringUtil.trans(map.get("i_safezone")));
                            dirField.add(StringUtil.trans(map.get("i_dir")));
                            masterField.add(StringUtil.trans(map.get("i_master")));
                            slaveField.add(StringUtil.trans(map.get("i_slave")));
                            ext_ser_idField.add(StringUtil.trans(map.get("i_ext_ser_id")));
                        }
                        Data_typeField.append(data_typeField);
                        Sender_idField.append(sender_idField);
                        Data_pt_idField.append(data_ptField);
                        MsgtypeField.append(msgtypeField);
                        Sn_toField.append(sn_toField);
                        Sn_fromField.append(sn_fromField);
                        SafezoneField.append(safezoneField);
                        DirField.append(dirField);
                        MasterField.append(masterField);
                        SlaveField.append(slaveField);
                        Ext_ser_idField.append(ext_ser_idField);
                        return null;
                    }
                }
        ).collect();
    }
}
