package util.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Config;
import util.Time;

import java.io.*;
import java.util.*;

public class ResultBackup implements Serializable {
    /**
     * ResultBackup 类
     * 学习模式保存操作码规则（学习结果）至本地的方法类
     * 包括保存文件、移除重复键等功能
     */
    Logger logger = LoggerFactory.getLogger(ResultBackup.class);
    private final String dirPath = Config.getFileDirPath() + "/";

    /**
     * save()
     * 保存流程的入口函数
     * 保存文件内容至本地的方法类 （为保证并发可靠性使用了单一线程抢占标识）
     * @param hotKeyMap 待保存的学习结果
     */
    synchronized public void save(Map<String, List<List<String>>> hotKeyMap) {
        if (hotKeyMap == null)
            return;
        List<String> elasticsearchField = Config.getElasticsearchFields();
        for (String str: elasticsearchField) {
            saveFile(str, hotKeyMap.get(str));
        }
    }

    /**
     * removeRepeatKey()
     * 保存时对已有规则进行检测，如果出现了重复规则，则移除
     * @param hotKeyLists 操作码
     * @return 移除后的操作码
     */
    private List<List<String>> removeRepeatKey (List<List<String>> hotKeyLists) {
        if (hotKeyLists == null)
            return null;
        return new ArrayList<>(new HashSet<>(hotKeyLists));
    }

    /**
     * saveFile()
     * 保存内容至文件的方法
     * @param fileName 文件名（地址）
     * @param hotKeyLists 保存内容
     */
    synchronized public void saveFile (String fileName, List<List<String>> hotKeyLists) {
        hotKeyLists = removeRepeatKey(hotKeyLists);
        if (fileName == null) {
//            System.out.println("DPDST");
            return;
        }

        if (hotKeyLists == null) {
//            System.out.println("hotkey nul 01");
            return;
        }

        if (hotKeyLists.size() == 0) {
//            System.out.println("hotkey null 02");
            return;
        }


        String filePath = dirPath + fileName + ".iedb";
        File file = new File(filePath);

        try {
            if(!file.exists())
                file.createNewFile();

            BufferedWriter fileWriter = new BufferedWriter(new FileWriter(file));

            for (List<String> list: hotKeyLists) {
                for (String str : list) {
                    fileWriter.append(str);
                    fileWriter.append(',');
                }
                fileWriter.append("\r\n");
            }
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            System.out.println("Not save");
            logger.error(Long.toString(Time.now()));
            logger.error(e.toString());
        }
    }
}
