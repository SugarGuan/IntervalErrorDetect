package core;

import core.clean.CleanRedis;

public class Clean {
    public void execute() {
        CleanRedis cleanRedis = new CleanRedis();
        cleanRedis.cleanRecords();
    }
}
