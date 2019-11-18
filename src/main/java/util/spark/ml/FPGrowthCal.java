package util.spark.ml;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

import java.util.List;

public class FPGrowthCal extends SparkML {
    /**
     * FPGrowth算法类
     * SparkML工具集下的FPGrowth算法，可以提供一种对不重复序列各元素间关联程度的推演，挖掘出不连贯队列间的潜在关联性
     * 具体包括前后关联性（P(b|a)）、P(ab)
     */
    private static double minSupport ;
    private static double minConfidence ;
    static int numPartition ;

    /**
     * execute()
     * 执行FPGrowth算法，挖掘队列元素关联
     * minSupport表示算法所需的最小支持度
     * minConfidence表示算法所需的最小置信度
     * numPartition表示最小分区
     * @param rdd 返回结果RDD
     */
    public static void execute(JavaRDD<List<String>> rdd) {
        minSupport = 0.2;
        minConfidence = 0.8;
        numPartition = 10;
        System.out.println("--------------------------------------------------------------");
        System.out.println(rdd.count());
        System.out.println("--------------------------------------------------------------");

        FPGrowth fpg = new FPGrowth().setMinSupport(minSupport).setNumPartitions(numPartition);
        FPGrowthModel<String> model = fpg.run(rdd);
        for (FPGrowth.FreqItemset<String> itemset : model.freqItemsets().toJavaRDD().collect()) {
            System.out.println("[" + itemset.javaItems() + "]," + itemset.freq());
        }

        System.out.println("--------------------------------------------------------------");


        for (AssociationRules.Rule<String> rule : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
            System.out.println(rule.javaAntecedent() + "=>" + rule.javaConsequent() + "," + rule.confidence());
        }
    }
}
