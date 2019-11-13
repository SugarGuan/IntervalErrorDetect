package core;

import core.clean.LocalCleaner;

public class Clean {
    public void autorun() {
        execute();
    }

    public void execute() {
        LocalCleaner localCleaner = new LocalCleaner();
        localCleaner.clean();
    }
}
