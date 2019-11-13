package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Config {
    private static String configFileAddress = "D:\\Project\\2020\\conf.conf";
    private static String codingBy = "UTF-8";
    private static Map<String, Integer> dialogIndexMap;
    private static List<String> elasticsearchIndices;
    private static List<String> elasticsearchFields;
    private static double frequentPercentage = 0;

    static {
        defaultSetting();
    }

    public Config() {
        defaultSetting();
    }

    public Config(String configFileAddr) {
        configFileAddress = configFileAddr;
        defaultSetting();
    }

    public Config(String configFileAddr, String coding) {
        configFileAddress = configFileAddr;
        codingBy = coding;
        defaultSetting();
    }

    private static void defaultSetting(){
        setDialogIndexMap();
        setElasticSearchIndices();
        setElasticsearchFields();
    }

    private static void setDialogIndexMap() {

        dialogIndexMap = new HashMap<>();
        // 1 : AppName + source IP + destination IP
        // 2 : AppName + source macAddr + destination macAddr

        dialogIndexMap.put("au_pkt_ams", 1);
        dialogIndexMap.put("au_pkt_arp", 2);
        dialogIndexMap.put("au_pkt_bacnet", 1);
        dialogIndexMap.put("au_pkt_cip", 1);
        dialogIndexMap.put("au_pkt_coap", 1);
        dialogIndexMap.put("au_pkt_dnp3", 1);
        dialogIndexMap.put("au_pkt_dns", 1);
        dialogIndexMap.put("au_pkt_dsi", 1);
        dialogIndexMap.put("au_pkt_egd", 1);
        dialogIndexMap.put("au_pkt_eplv1", 2);
        dialogIndexMap.put("au_pkt_es", 1);
        dialogIndexMap.put("au_pkt_esio", 1);
        dialogIndexMap.put("au_pkt_ethercat", 2);
        dialogIndexMap.put("au_pkt_ethernetip", 1);
        dialogIndexMap.put("au_pkt_ffhse", 1);
        dialogIndexMap.put("au_pkt_fox", 1);
        dialogIndexMap.put("au_pkt_ftp", 1);
        dialogIndexMap.put("au_pkt_goose", 2);
        dialogIndexMap.put("au_pkt_gryphon", 1);
        dialogIndexMap.put("au_pkt_hartip", 1);
        dialogIndexMap.put("au_pkt_https", 1);
        dialogIndexMap.put("au_pkt_iec104", 1);
        dialogIndexMap.put("au_pkt_imap", 1);
        dialogIndexMap.put("au_pkt_influx", 1);
        dialogIndexMap.put("au_pkt_irc", 1);
        dialogIndexMap.put("au_pkt_lldp", 2);
        dialogIndexMap.put("au_pkt_llmnr", 1);
        dialogIndexMap.put("au_pkt_lontalk", 1);
        dialogIndexMap.put("au_pkt_mdns", 1);
        dialogIndexMap.put("au_pkt_mms", 1);
        dialogIndexMap.put("au_pkt_modbus", 1);
        dialogIndexMap.put("au_pkt_mysql", 1);
        dialogIndexMap.put("au_pkt_oicq", 1);
        dialogIndexMap.put("au_pkt_omronfins", 1);
        dialogIndexMap.put("au_pkt_opc", 1);
        dialogIndexMap.put("au_pkt_opcua", 1);
        dialogIndexMap.put("au_pkt_opensafety", 1);
        dialogIndexMap.put("au_pkt_pgsql", 1);
        dialogIndexMap.put("au_pkt_powerlink", 2);
        dialogIndexMap.put("au_pkt_s7plus", 1);
        dialogIndexMap.put("au_pkt_sercosiii", 2);
        dialogIndexMap.put("au_pkt_sf_hasp", 1);
        dialogIndexMap.put("au_pkt_ssdp", 1);

    }

    public static int getDialogIndexMapValue(String index) {
        if (dialogIndexMap == null)
            setDialogIndexMap();
        if (dialogIndexMap.get(index) == null)
            return 0;
        return dialogIndexMap.get(index);
    }

    private static void setElasticSearchIndices() {
        elasticsearchIndices = new ArrayList<>();
        elasticsearchIndices.add("au_pkt_ams");
        elasticsearchIndices.add("au_pkt_arp");
        elasticsearchIndices.add("au_pkt_bacnet");
//        elasticsearchIndices.add("au_pkt_cip");
//        elasticsearchIndices.add("au_pkt_coap");
//        elasticsearchIndices.add("au_pkt_dnp3");
//        elasticsearchIndices.add("au_pkt_dns");
//        elasticsearchIndices.add("au_pkt_dsi");
//        elasticsearchIndices.add("au_pkt_egd");
//        elasticsearchIndices.add("au_pkt_eplv1");
//        elasticsearchIndices.add("au_pkt_es");
//        elasticsearchIndices.add("au_pkt_esio");
//        elasticsearchIndices.add("au_pkt_ethercat");
//        elasticsearchIndices.add("au_pkt_ethernetip");
//        elasticsearchIndices.add("au_pkt_ffhse");
//        elasticsearchIndices.add("au_pkt_fox");
//        elasticsearchIndices.add("au_pkt_ftp");
//        elasticsearchIndices.add("au_pkt_goose");
//        elasticsearchIndices.add("au_pkt_gryphon");
//        elasticsearchIndices.add("au_pkt_hartip");
//        elasticsearchIndices.add("au_pkt_https");
//        elasticsearchIndices.add("au_pkt_iec104");
//        elasticsearchIndices.add("au_pkt_imap");
//        elasticsearchIndices.add("au_pkt_influx");
//        elasticsearchIndices.add("au_pkt_irc");
//        elasticsearchIndices.add("au_pkt_lldp");
//        elasticsearchIndices.add("au_pkt_llmnr");
//        elasticsearchIndices.add("au_pkt_lontalk");
//        elasticsearchIndices.add("au_pkt_mdns");
//        elasticsearchIndices.add("au_pkt_mms");
//        elasticsearchIndices.add("au_pkt_modbus");
//        elasticsearchIndices.add("au_pkt_mysql");
//        elasticsearchIndices.add("au_pkt_oicq");
//        elasticsearchIndices.add("au_pkt_omronfins");
//        elasticsearchIndices.add("au_pkt_opc");
//        elasticsearchIndices.add("au_pkt_opcua");
//        elasticsearchIndices.add("au_pkt_opensafety");
//        elasticsearchIndices.add("au_pkt_pgsql");
//        elasticsearchIndices.add("au_pkt_powerlink");
//        elasticsearchIndices.add("au_pkt_s7plus");
//        elasticsearchIndices.add("au_pkt_sercosiii");
//        elasticsearchIndices.add("au_pkt_sf_hasp");
//        elasticsearchIndices.add("au_pkt_ssdp");
    }

    public static List<String> getElasticsearchIndices() {
        return elasticsearchIndices;
    }

    private static void setElasticsearchFields() {
        elasticsearchFields = new ArrayList<>();
        elasticsearchFields.add("addr");
        elasticsearchFields.add("appid");
        elasticsearchFields.add("cmd");
        elasticsearchFields.add("cmd_str");
        elasticsearchFields.add("code");
        elasticsearchFields.add("commaddr");
        elasticsearchFields.add("confirm");
        elasticsearchFields.add("conf_rev");
        elasticsearchFields.add("content");
        elasticsearchFields.add("context");
        elasticsearchFields.add("daddr");
        elasticsearchFields.add("data");
        elasticsearchFields.add("datalen");
        elasticsearchFields.add("data_hdr");
        elasticsearchFields.add("data_pt_id");
        elasticsearchFields.add("data_type");
        elasticsearchFields.add("datset");
        elasticsearchFields.add("dest");
        elasticsearchFields.add("dest_str");
        elasticsearchFields.add("dir");
        elasticsearchFields.add("domain");
        elasticsearchFields.add("dst");
        elasticsearchFields.add("dst_ch");
        elasticsearchFields.add("entry_num");
        elasticsearchFields.add("exchange_id");
        elasticsearchFields.add("ext_ser_id");
        elasticsearchFields.add("fda_addr");
        elasticsearchFields.add("flag");
        elasticsearchFields.add("frame_type");
        elasticsearchFields.add("func");
        elasticsearchFields.add("function");
        elasticsearchFields.add("gocbref");
        elasticsearchFields.add("goid");
        elasticsearchFields.add("groupnum");
        elasticsearchFields.add("hdr_flag");
        elasticsearchFields.add("hostname");
        elasticsearchFields.add("infoaddr");
        elasticsearchFields.add("interface");
        elasticsearchFields.add("ip");
        elasticsearchFields.add("json");
        elasticsearchFields.add("layer");
        elasticsearchFields.add("loc");
        elasticsearchFields.add("master");
        elasticsearchFields.add("method");
        elasticsearchFields.add("msgid");
        elasticsearchFields.add("msgtype");
        elasticsearchFields.add("msgtype_str");
        elasticsearchFields.add("ndscom");
        elasticsearchFields.add("node_d");
        elasticsearchFields.add("node_s");
        elasticsearchFields.add("n_data");
        elasticsearchFields.add("offset_addr");
        elasticsearchFields.add("offset_errcode");
        elasticsearchFields.add("pkt_type");
        elasticsearchFields.add("producer_id");
        elasticsearchFields.add("proto");
        elasticsearchFields.add("qqid");
        elasticsearchFields.add("rdn");
        elasticsearchFields.add("reqid");
        elasticsearchFields.add("reqtype");
        elasticsearchFields.add("req_id");
        elasticsearchFields.add("saddr");
        elasticsearchFields.add("safezone");
        elasticsearchFields.add("sender_id");
        elasticsearchFields.add("server");
        elasticsearchFields.add("service");
        elasticsearchFields.add("sid");
        elasticsearchFields.add("slave");
        elasticsearchFields.add("slave_addr");
        elasticsearchFields.add("sn_from");
        elasticsearchFields.add("sn_to");
        elasticsearchFields.add("source");
        elasticsearchFields.add("source_str");
        elasticsearchFields.add("sqnum");
        elasticsearchFields.add("src");
        elasticsearchFields.add("src_cid");
        elasticsearchFields.add("src_statid");
        elasticsearchFields.add("status");
        elasticsearchFields.add("stnum");
        elasticsearchFields.add("subnet_d");
        elasticsearchFields.add("subnet_s");
        elasticsearchFields.add("tel_id");
        elasticsearchFields.add("test");
        elasticsearchFields.add("token");
        elasticsearchFields.add("trans_id");
        elasticsearchFields.add("type");
        elasticsearchFields.add("url");
        elasticsearchFields.add("varnum");
        elasticsearchFields.add("vendor_code");
        elasticsearchFields.add("ver");
    }

    public static List<String> getElasticsearchFields() {
        return elasticsearchFields;
    }

    private static String retrieve(String key) {
        if (configFileAddress == null) {
            return null;
        }

        File file = new File(configFileAddress);
        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            return null;
        }
        String str = null;
        String keyFromFile = null;
        int cutter = 0;
        try {
            while (null != (str = bufferedReader.readLine())) {
                cutter = str.indexOf("=");
                if (cutter <= 0)
                    continue;
                keyFromFile = str.substring(0, cutter);
                if (keyFromFile.equals(key)) {
                    bufferedReader.close();
                    return str.substring(cutter + 1);
                }
            }
            bufferedReader.close();
        } catch (Exception e) {
            return null;
        }
        return null;
    }

    public static String getSparkNoticeLevel() {
        String sparkNoticeLevel = retrieve("sparkNoticeLevel");
        if (null != sparkNoticeLevel)
            return sparkNoticeLevel;
        return "OFF";
    }

    public static double getFrequentPercentage () {
        String frequentPercentageFromFile = retrieve("fp");
        if (null == frequentPercentageFromFile)
            return 0;
        try {
            return Double.parseDouble(frequentPercentageFromFile);
        } catch (Exception e) {
            return 0;
        }
    }
}
