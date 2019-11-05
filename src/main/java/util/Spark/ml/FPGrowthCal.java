package util.Spark.ml;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

import java.util.List;

public class FPGrowthCal extends SparkML {
    private static double minSupport ;
    private static double minConfidence ;
    static int numPartition ;

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
