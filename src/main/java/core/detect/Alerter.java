package core.detect;

import java.io.Serializable;

public class Alerter implements Serializable {
    public void report(String timestamp, String appName) {
        System.out.println("Founded.");
    }
}
