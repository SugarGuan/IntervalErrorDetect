package core;

import dao.elsaticsearch.ElasticSearch;
import dao.redis.Redis;
import org.apache.spark.SparkException;

public class Switch {
    private StringBuffer nowFlag = new StringBuffer();
    private StringBuffer pastFlag = new StringBuffer();
    private Runnable runnable;
    private Thread thread;
    private Redis redis = new Redis();
    private ElasticSearch es;


    /**
     * Switch 类
     * 该类提供下述功能：
     * 1.将当前执行的模式线程interrupt（置中段位）
     * 2.创建对应模式的线程并切换至启动对应模式的逻辑
     * 3.如果对应模式只执行一次，则负责接管切换回原模式的逻辑
     * 4.切换模式是根据websocket发来的指令消息切换，负责向redis发送指令响应消息
     */


    /**
     * ElasticSearch对象的依赖注入
     * @param es elasticsearch数据源操作对象
     */

    public Switch(ElasticSearch es){
        this.es = es;
    }

    /**
     * 该方法由client对象调用。client接受websocket发送的json消息并进行模式切换。
     * @param mode
     */

    /**
     * 根据传入的mode值确定切换的模式，停止当前模式的线程，切换至对应模式的线程
     * @param mode 模式名（规则 on off clear）
     */
    public void autoSwitch (String mode) {
        // Initial Mode
        /**
         *  autoSwitch 自动切换方法逻辑：
         *  D代表检测模式Detecting mode / C 代表清理模式Cleaning mode / L 代表学习模式 Learning mode
         *  程序启动时进入检测模式（对应方法调用已由client autoSwitch中明确）
         *
         *  1. 确定收入的模式信息，其中模式信息on代表 study on：开启学习模式 / off 代表 study off， 关闭学习模式 /
         *  clear 代表 study clear， 清除学习模式生成的规则文件
         *  2. 将模式文本更换为较为简单的形式，增强可读性，使用其他String变量（modeCorrected）表示当前状态。
         *  3. 根据前序状态和现在状态，向redis发送对应的程序响应，响应当前程序的功能切换结果
         *  4. 根据传入的，需要切换的模式执行不同逻辑：
         *  4.1 清理模式 C ：中断当前模式线程，获取清理模式线程并启动；不更新当前线程、前序线程记录位。
         *  4.2 学习模式 L ：中断当前模式线程，前序线程记录位记录当前线程，当前线程记录位记录 L ，获取学习模式线程并启动
         *  4.3 检测模式 D ：中断当前模式线程，前序线程记录位记录当前线程，当前线程记录位记录 D ，获取检测模式线程并启动
         */
        String modeCorrected = "D";
        if (mode.equals("on")) {
            modeCorrected = "L";
            redis.insertRedisList("cmd_dis_result", "{\"app\":\"ad_cycledetect\",\"op\":\"learn\",\"state\":\"starting\"}");
        }
        else if (mode.equals("clear")) {
            modeCorrected = "C";
        }
        else if (mode.equals("off")){
            redis.insertRedisList("cmd_dis_result", "{\"app\":\"ad_cycledetect\",\"op\":\"learn\",\"state\":\"stopping\"}");
        }
        else
            return;

        if (modeCorrected.equals(nowFlag.toString()))
            return;

        // Update Mode Flags;
        if (modeCorrected.equals("C")) {
            thread.interrupt();
            runnable = getModeRuuable("C");
            thread = new Thread(runnable);
            thread.start();
            runnable = getModeRuuable(nowFlag.toString());
            thread = new Thread(runnable);
            thread.start();
            return ;
        }

        if (modeCorrected.equals("L")) {
            pastFlag.setLength(0);
            pastFlag.append(nowFlag.toString());
            nowFlag.setLength(0);
            nowFlag.append(modeCorrected);
            thread.interrupt();
            runnable = getModeRuuable("L");
            thread = new Thread(runnable);
            thread.start();
            return ;
        }

        pastFlag.setLength(0);
        pastFlag.append(nowFlag.toString());
        nowFlag.setLength(0);
        nowFlag.append(modeCorrected);
        if (thread != null)
            thread.interrupt();
        runnable = getModeRuuable("D");
        thread = new Thread(runnable);
        thread.start();

    }

    /**
     * getModeRunnable()
     * 方法根据提供的模式信息返回对应模式的执行线程逻辑（runnable对象）
     * @param mode 模式名
     * @return 对应模式的Runnable对象
     */
    private Runnable getModeRuuable (String mode) {
        Learn learner = new Learn(es);
        Clean cleaner = new Clean();
        Detect detector = new Detect(es);
        Runnable learn = () -> {
            try {
                redis.insertRedisList("cmd_dis_result", "{\"app\":\"ad_cycledetect\",\"op\":\"learn\",\"state\":\"on\"}");
                while (!Thread.currentThread().isInterrupted()) {
                    System.out.println("Learning mode");
                    learner.autorun();
                }
                System.out.println("Learning mode off");
            } catch (Exception e) {

            }
        };
        Runnable clean = () -> {
            System.out.println("Cleaning mode");
            cleaner.autorun();
        };
        Runnable detect = () -> {
            try {
                redis.insertRedisList("cmd_dis_result", "{\"app\":\"ad_cycledetect\",\"op\":\"learn\",\"state\":\"off\"}");
                while (!Thread.currentThread().isInterrupted()) {
                    System.out.println("Detecting mode");
                    detector.autorun();
                }
                System.out.println("Detecting mode off");
            } catch (Exception e) {
//                System.out.println("Thread running error: Excepted interrupt.");
//                System.out.println("Error occurs when application running " + Time.timeFormatEnglish(Time.now() - start) + "later.");
            }
        };
        if (mode.equals("C"))
            return clean;
        if (mode.equals("L"))
            return learn;
        return detect;
    }

}
