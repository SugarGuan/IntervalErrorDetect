package core.learn.module.cross.index;

import core.learn.field.*;
import core.learn.module.Module;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class EsioModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_esio");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> pkt_typeField;
                    List<String> verField;
                    List<String> trans_idField;
                    List<String> tel_idField;
                    List<String> src_statField;
                    List<String> n_dataField;
                    List<String> hdr_flagField;
                    List<String> data_hdrField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        pkt_typeField = new ArrayList<>();
                        verField = new ArrayList<>();
                        trans_idField = new ArrayList<>();
                        tel_idField = new ArrayList<>();
                        src_statField = new ArrayList<>();
                        n_dataField = new ArrayList<>();
                        hdr_flagField = new ArrayList<>();
                        data_hdrField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            pkt_typeField.add((String) map.get("i_pkt_type"));
                            verField.add((String) map.get("i_ver"));
                            trans_idField.add((String) map.get("i_trans_id"));
                            tel_idField.add((String) map.get("i_tel_id"));
                            src_statField.add((String) map.get("i_src_statid"));
                            n_dataField.add((String) map.get("i_n_data"));
                            hdr_flagField.add((String) map.get("i_hdr_flag"));
                            data_hdrField.add((String) map.get("i_data_hdr"));
                        }
                        Pkt_typeField.append(pkt_typeField);
                        VerField.append(verField);
                        Trans_idField.append(trans_idField);
                        Tel_idField.append(tel_idField);
                        Src_statidField.append(src_statField);
                        DataField.append(n_dataField);
                        Hdr_flagField.append(hdr_flagField);
                        Data_hdrField.append(data_hdrField);
                        return null;
                    }
                }
        ).collect();
    }
}
