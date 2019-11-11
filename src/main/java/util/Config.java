package util;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Config {
    private static String configFileAddress = "D:\\Project\\2020\\conf.conf";
    private static String codingBy = "UTF-8";
    private static Map<String, Integer> dialogIndexMap;
    private static List<String> elasticsearchIndices;
    private static double hotkeyAppearancePercentage;

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
        setHotKeyAppearancePercentage();
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

    private static void setHotKeyAppearancePercentage() {
        String hotKeyAppearancePercentage = retrieve("HotKeyAppearancePercentage");
        double hotkey = 0;
        try {
            if (hotKeyAppearancePercentage != null)
                hotkey = Double.parseDouble(hotKeyAppearancePercentage);
        } catch (Exception e) {
            hotkey = 0.4;
        }
        hotkeyAppearancePercentage = hotkey;
    }

    public static double getHotkeyAppearancePercentage() {
        return hotkeyAppearancePercentage;
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

    //
    public static String getSparkNoticeLevel() {
        String sparkNoticeLevel = retrieve("sparkNoticeLevel");
        if (null != sparkNoticeLevel)
            return sparkNoticeLevel;
        return "OFF";
    }
}
