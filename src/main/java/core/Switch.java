package core;

import dao.redis.Redis;
import util.Config;
import util.websocket.Client;

public class Switch {
    private StringBuffer nowFlag = new StringBuffer();
    private StringBuffer pastFlag = new StringBuffer();
    private Runnable runnable;
    private Thread thread;

    public void autoSwitch (String mode) {
        // Initial Mode
        String modeCorrected = "D";
        if (mode.equals("on"))
            modeCorrected = "L";
        else if (mode.equals("clean"))
            modeCorrected = "C";

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

    private Runnable getModeRuuable (String mode) {
        Learn learner = new Learn();
        Clean cleaner = new Clean();
        Detect detector = new Detect();
        Runnable learn = () -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    System.out.println("Learning mode");
                    learner.autorun();
                }
                System.out.println("Learning mode off");
            } catch (InterruptedException e) {

            }
        };
        Runnable clean = () -> {
            System.out.println("Cleaning mode");
            cleaner.autorun();
        };
        Runnable detect = () -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    System.out.println("Detecting mode");
                    detector.autorun();
                }
                System.out.println("Detecting mode off");
            } catch (InterruptedException e) {
//                System.out.println("Thread running error: Excepted interrupt.");
//                System.out.println("Error occurs when application running " + Time.timeFormatEnglish(Time.now() - start) + "later.");
            }
        };
        if (mode == "C")
            return clean;
        if (mode == "L")
            return learn;
        return detect;
    }

    private void modeSwitchResponse(String mode) {
        Redis redis = new Redis();
        if (mode == "C") {
            redis.insertRedisList("cmd_dis_result", "");
            return ;
        }

        if (mode == "L") {
            redis.insertRedisList("cmd_dis_result", "");
            return ;
        }

        redis.insertRedisList("cmd_dis_result", "");
    }
}
