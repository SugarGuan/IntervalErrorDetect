package core.clean;

import util.Config;

import java.io.File;

public class LocalCleaner {

    /**
     * LocalCleaner 类
     * Clean模式的核心类，用于清理本地学习到的规则文件。
     */


    public boolean clean() {
        /**
         * clean()方法用于删除规则文件夹下的所有文件，具体逻辑是：
         *
         * 1.获取规则文件路径
         * 1.1 如果规则文件路径为null，返回true标识，表示删除成功
         * 1.2 如果规则文件路径为""，返回true表示，表示删除成功
         *
         * 2.创建File对象
         * 2.1 如果File不存在，表示规则文件路径指向的文件实体（规则路径文件夹）不存在，无需删除，返回true
         *
         * 3.File存在
         * 3.1 如果File是文件夹，获取File下文件列表，逐一删除，返回状态值。（每个文件删除状态取并，如果有一个失败就是false）
         * 3.2 如果File是文件（不是文件夹），删除File后返回删除是否成功的状态值
         *
         * **/
        String filePath = Config.getFileDirPath();
        if (filePath == null){
            return true;
        }
        if (filePath.equals("")) {
            return true;
        }

        File file = new File(filePath);

        if (!file.exists())
        {
            return true;
        }
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            boolean result = true;
            for (File f :files) {
                result = ( result && f.delete() );
            }
            return result;
        }
        else {
            return file.delete();
        }
    }
}
