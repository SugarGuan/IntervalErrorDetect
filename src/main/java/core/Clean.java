package core;

import core.clean.LocalCleaner;

public class Clean {
    public void autorun() {
        execute();
    }

    public void execute() {
        LocalCleaner localCleaner = new LocalCleaner();
        if (localCleaner.clean())
            System.out.println("Deleted");
        else
            System.out.println("Not deleted.");
    }
}
