import core.Clean;
import core.Detect;
import core.Learn;
import util.Time;

public class Main {
    public static int count = 0;
    public static void main(String[] args) {
        Long start = Time.now();
        Learn learner = new Learn();
        Clean cleaner = new Clean();
        Detect detector = new Detect();
        Runnable learn = () -> {
          try {
              learner.autorun();
          }  catch (InterruptedException e) {
              System.out.println("Thread running error: Excepted interrupt.");
              System.out.println("Error occurs when application running " + Time.timeFormatEnglish(Time.now() - start) + "later.");
          }

        };
        Runnable clean = () -> {
            cleaner.autorun();
        };
        Runnable detect = () -> {
            try{
                detector.autorun();
            } catch (InterruptedException e) {
                System.out.println("Thread running error: Excepted interrupt.");
                System.out.println("Error occurs when application running " + Time.timeFormatEnglish(Time.now() - start) + "later.");
            }
        };
// Temporal codes:
        Runnable detect2 = () -> {
            try {
                detector.execute();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };

        try{
            Thread t = new Thread(learn);
            t.start();
            Thread.sleep(50 * 1000);
            System.out.println("Mode Interrupted : \"detecting Mode.\"");
            t.interrupt();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
