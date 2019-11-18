package util.spark;

import util.Config;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Spark {

    /**
     *  Spark 类
     *  提供Spark操作的封装桥函数 方法类
     *  配置Spark配置项 （SparkConf）
     *  生成全局唯一的Spark Context
     */

    private static String sparkRunMode = "local";                              // Default setting to local.
    private static String sparkAppname = "SparkES";                            // Default setting to SparkES.
    private static String sparkBrodacastCompress = "true";                     // Default OPEN to speed up the plugin.
    private static String sparkRDDCompress = "true";                           // Default OPEN to speed up the plugin.
    private static String sparkIOCompressJarSourcePath = "org.apache.spark.io.LZFCompressionCodec";
    private static String sparkShuffleFileBuffer = "1280k";
    private static String sparkReducerMaxSizeInFlight = "1024m";
    private static String sparkReducerMaxMblnFlight = "1024m";
    private static SparkConf sparkConf;
    private static JavaSparkContext sc;                                        // Only one SparkContext over-all.

    static {
        setSparkConf();
    }

    private static void setSparkConf(){
        sparkConf = new SparkConf();
        sparkConf.setMaster(sparkRunMode);
        sparkConf.setAppName(sparkAppname);
        sparkConf.set("spark.rdd.compress", sparkRDDCompress);
        sparkConf.set("spark.broadcast.compress", sparkBrodacastCompress);
        sparkConf.set("spark.io.compression.codec", sparkIOCompressJarSourcePath);
        sparkConf.set("spark.shuffle.file.buffer", sparkShuffleFileBuffer);
        sparkConf.set("spark.reducer.maxSizeInFlight", sparkReducerMaxSizeInFlight);
        sparkConf.set("spark.reducer.maxMblnFlight", sparkReducerMaxMblnFlight);
    }

    public static SparkConf getSparkConf() {
        if (sparkConf != null)
            return sparkConf;
        setSparkConf();
        return sparkConf;
    }

    public static void appendSparkConf(String configKey, String configValue) {
        if (sparkConf == null)
            setSparkConf();
        sparkConf.set(configKey, configValue);
    }

    public static JavaSparkContext getSparkContext() {
        if(sc != null)
            return sc;
        sc = new JavaSparkContext(sparkConf);
        setLogLevel();
        return sc;
    }


    private static void setLogLevel() {
        String configFileValue = Config.getSparkNoticeLevel().toLowerCase();
        /**
         * @value:   configFileValue
         * off : close all notices
         * info : showing all information
         * error : showing out any errors but no these execute information
         * DEFAULT: info
         * **/
        if (configFileValue.equals("off")) {
            sc.setLogLevel("OFF");
            Logger.getLogger("org").setLevel(Level.OFF);
            Logger.getLogger("org.apache.spark").setLevel(Level.OFF);
            Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF);
            return ;
        }

        if (configFileValue.equals("error")) {
            sc.setLogLevel("ERROR");
            return ;
        }

        sc.setLogLevel("INFO");

    }
}
