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
    private static String fileDirPath;
    private static Map<String, List<String>> elasticSearchIndexFieldDict;

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
        setElasticSearchIndexFieldDict();
        setFileDirPath();
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
//        elasticsearchIndices.add("au_pkt_arp");
//        elasticsearchIndices.add("au_pkt_bacnet");
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

    private static void setElasticsearchFields () {
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

    public static List<String> getElasticsearchFields () {
        return elasticsearchFields;
    }

    private static void setElasticSearchIndexFieldDict () {
        elasticSearchIndexFieldDict = new HashMap<>();
        List<String> field = new ArrayList<>();

        field.add("i_cmd");
        elasticSearchIndexFieldDict.put("au_pkt_ams", field);
        field = new ArrayList<>();

        field.add("i_pkt_type");
        elasticSearchIndexFieldDict.put("au_pkt_arp", field);
        field = new ArrayList<>();

        field.add("i_data_type");
        field.add("i_func");
        elasticSearchIndexFieldDict.put("au_pkt_bacnet", field);
        field = new ArrayList<>();

        field.add("pkt_type");
        field.add("i_service");
        elasticSearchIndexFieldDict.put("au_pkt_cip", field);
        field = new ArrayList<>();

        field.add("i_pkt_type");
        field.add("i_code");
        field.add("i_msgid");
        field.add("i_token");
        elasticSearchIndexFieldDict.put("au_pkt_coap", field);
        field = new ArrayList<>();

        field.add("i_saddr");
        field.add("i_daddr");
        field.add("i_func");
        field.add("i_groupnum");
        field.add("i_varnum");
        elasticSearchIndexFieldDict.put("au_pkt_dnp3", field);
        field = new ArrayList<>();

        field.add("i_domain");
        elasticSearchIndexFieldDict.put("au_pkt_dns", field);
        field = new ArrayList<>();

        field.add("i_flag");
        field.add("i_cmd");
        field.add("i_reqid");
        field.add("i_offset_errcode");
        field.add("i_datalen");
        elasticSearchIndexFieldDict.put("au_pkt_dsi", field);
        field = new ArrayList<>();

        field.add("i_req_id");
        field.add("i_producer_id");
        field.add("i_exchange_id");
        field.add("i_status");
        field.add("i_datalen");
        elasticSearchIndexFieldDict.put("au_pkt_egd", field);
        field = new ArrayList<>();

        field.add("i_service");
        field.add("i_dest");
        field.add("i_source");
        field.add("i_cmd");
        elasticSearchIndexFieldDict.put("au_pkt_eplv1", field);
        field = new ArrayList<>();

        field.add("i_pkt_type");
        field.add("i_json");
        elasticSearchIndexFieldDict.put("au_pkt_es", field);
        field = new ArrayList<>();

        field.add("i_pkt_type");
        field.add("i_ver");
        field.add("i_trans_id");
        field.add("i_tel_id");
        field.add("i_src_statid");
        field.add("i_n_data");
        field.add("i_hdr_flag");
        elasticSearchIndexFieldDict.put("au_pkt_esio", field);
        field = new ArrayList<>();

        field.add("i_data_hdr");
        field.add("i_cmd");
        field.add("i_cmd_str");
        field.add("i_slave_addr");
        elasticSearchIndexFieldDict.put("au_pkt_ethercat", field);
        field = new ArrayList<>();

        field.add("i_offset_addr");
        field.add("pkt_type");
        elasticSearchIndexFieldDict.put("au_pkt_ethernetip", field);
        field = new ArrayList<>();

        field.add("i_proto");
        field.add("i_msgtype");
        field.add("i_confirm");
        field.add("i_service");
        field.add("i_fda_addr");
        elasticSearchIndexFieldDict.put("au_pkt_ffhse", field);
        field = new ArrayList<>();

        field.add("i_data");
        elasticSearchIndexFieldDict.put("au_pkt_fox", field);
        field = new ArrayList<>();

        field.add("i_pkt_type");
        elasticSearchIndexFieldDict.put("au_pkt_ftp", field);
        field = new ArrayList<>();

        field.add("i_appid");
        field.add("i_gocbref");
        field.add("i_datset");
        field.add("i_goid");
        field.add("i_stnum");
        field.add("i_sqnum");
        field.add("i_test");
        field.add("i_conf_rev");
        field.add("i_ndscom");
        field.add("i_ndscom");
        field.add("i_datalen");
        elasticSearchIndexFieldDict.put("au_pkt_goose", field);
        field = new ArrayList<>();

        field.add("i_frame_type");
        field.add("i_src");
        field.add("i_src_cid");
        field.add("i_dst");
        field.add("i_dst_ch");
        field.add("i_cmd");
        field.add("i_context");
        field.add("i_datalen");
        elasticSearchIndexFieldDict.put("au_pkt_gryphon", field);
        field = new ArrayList<>();

        field.add("i_frame_type");
        field.add("i_addr");
        field.add("i_cmd");
        field.add("i_proto");
        elasticSearchIndexFieldDict.put("au_pkt_hartip", field);
        field = new ArrayList<>();

        field.add("i_rdn");
        elasticSearchIndexFieldDict.put("au_pkt_https", field);
        field = new ArrayList<>();

        field.add("i_commaddr");
        field.add("i_infoaddr");
        elasticSearchIndexFieldDict.put("au_pkt_iec104", field);
        field = new ArrayList<>();

        field.add("i_pkt_type");
        field.add("i_content");
        elasticSearchIndexFieldDict.put("au_pkt_imap", field);
        field = new ArrayList<>();

        field.add("i_pkt_type");
        field.add("i_url");
        elasticSearchIndexFieldDict.put("au_pkt_influx", field);
        field = new ArrayList<>();

        field.add("i_pkt_type");
        field.add("i_content");
        elasticSearchIndexFieldDict.put("au_pkt_irc", field);
        field = new ArrayList<>();

        field.add("i_data");
        elasticSearchIndexFieldDict.put("au_pkt_lldp", field);
        field = new ArrayList<>();

        field.add("i_ip");
        field.add("i_hostname");
        elasticSearchIndexFieldDict.put("au_pkt_llmnr", field);
        field = new ArrayList<>();

        field.add("i_vendor_code");
        field.add("i_sid");
        field.add("i_subnet_s");
        field.add("i_subnet_d");
        field.add("i_node_s");
        field.add("i_node_d");
        field.add("i_domain");
        elasticSearchIndexFieldDict.put("au_pkt_lontalk", field);
        field = new ArrayList<>();

        field.add("i_ip");
        field.add("i_hostname");
        elasticSearchIndexFieldDict.put("au_pkt_mdns", field);
        field = new ArrayList<>();

        field.add("i_type");
        elasticSearchIndexFieldDict.put("au_pkt_mms", field);
        field = new ArrayList<>();

        field.add("i_func");
        field.add("i_addr");
        elasticSearchIndexFieldDict.put("au_pkt_modbus", field);
        field = new ArrayList<>();

        field.add("i_pkt_type");
        elasticSearchIndexFieldDict.put("au_pkt_mysql", field);
        field = new ArrayList<>();

        field.add("i_cmd");
        field.add("i_qqid");
        elasticSearchIndexFieldDict.put("au_pkt_oicq", field);
        field = new ArrayList<>();
//        elasticSearchIndexFieldDict.put("au_pkt_omronfins", field);
//        field = new ArrayList<>();

        field.add("i_interface");
        field.add("i_method");
        elasticSearchIndexFieldDict.put("au_pkt_opc", field);
        field = new ArrayList<>();

        field.add("i_reqtype");
        elasticSearchIndexFieldDict.put("au_pkt_opcua", field);
        field = new ArrayList<>();

        field.add("i_data_type");
        field.add("i_sender_id");
        field.add("i_data_pt_id");
        field.add("i_msgtype");
        field.add("i_sn_to");
        field.add("i_sn_from");
        field.add("i_safezone");
        field.add("i_dir");
        field.add("i_master");
        field.add("i_slave");
        field.add("i_ext_ser_id");
        elasticSearchIndexFieldDict.put("au_pkt_opensafety", field);
        field = new ArrayList<>();

        field.add("i_pkt_type");
        elasticSearchIndexFieldDict.put("au_pkt_pgsql", field);
        field = new ArrayList<>();

        field.add("i_layer");
        field.add("i_msgtype");
        field.add("i_msgtype_str");
        field.add("i_dest");
        field.add("i_dest_str");
        field.add("i_source");
        field.add("i_source_str");
        elasticSearchIndexFieldDict.put("au_pkt_powerlink", field);
        field = new ArrayList<>();

        field.add("i_function");
        elasticSearchIndexFieldDict.put("au_pkt_s7plus", field);
        field = new ArrayList<>();
//        elasticSearchIndexFieldDict.put("au_pkt_sercosiii", field);
//        field = new ArrayList<>();

        field.add("i_proto");
        elasticSearchIndexFieldDict.put("au_pkt_sf_hasp", field);
        field = new ArrayList<>();

        field.add("i_pkt_type");
        field.add("i_ip");
        field.add("i_loc");
        field.add("i_server");
        elasticSearchIndexFieldDict.put("au_pkt_ssdp", field);
    }

    public static Map<String, List<String>> getElasticSearchIndexFieldDict () {
        return elasticSearchIndexFieldDict;
    }

    public static String getSparkNoticeLevel () {
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

    public static String getFileDirPath () {
        if (fileDirPath != null)
            setFileDirPath();
        return fileDirPath;
    }

    private static void setFileDirPath () {
        fileDirPath = retrieve("fileDirPath");
        if (fileDirPath == null)
            fileDirPath = "D:\\Project\\2020\\dig-lib";
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
}
