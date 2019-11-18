package core;

import core.clean.LocalCleaner;

public class Clean {
    /**
     *  Clean 类是Clear模式的控制器类，
     *  用于调用Clear工具（也可以按照需要扩展功能，如写入删除记录）
     */

    public void autorun() {
        execute();
    }

    public void execute() {
        LocalCleaner localCleaner = new LocalCleaner();
        localCleaner.clean();
    }
}
