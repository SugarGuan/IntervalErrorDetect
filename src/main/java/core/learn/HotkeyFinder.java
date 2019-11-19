package core.learn;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class HotkeyFinder implements Serializable {
    /**
     *  FieldHotKeyFinder 类
     *  是新版学习模式的核心类 （暂未启用）
     *  它从RDD中获取对应的队列，统计序列出现频次，存入规则文件（以对应field命名）
     */
    private int maxHotkeyLength = 20;
    private int minHotkeyLength = 3;
    private Long operationCount = 0L;
    public  Map<List<String >, Long> operationDictionary = new HashMap<>();
    public List<List<String> > operationLists = new ArrayList<>();

    public void appendOperationList (List<String> operationList) {
        if (operationLists == null)
            operationLists = new ArrayList<>();
        operationLists.add(operationList);
    }

    public void appendOperationLists (List<List<String>> operationLists) {
        if (operationLists == null)
            return;
        for (List<String> operationList: operationLists)
            appendOperationList(operationList);
    }

    public void slideWindowSublist (List<String> operationList) {
        if (operationList.size() == 0)
            return;
        List<String> tempList;
        int start = 0;
        int end = 0;
        int position = 0;
        int size = operationList.size();
        while (end < size) {
            start = end;
            while (start > -1 && start > (end - maxHotkeyLength)) {
                position = start;
                tempList = new ArrayList<>();
                while (position <= end) {
                    tempList.add(operationList.get(position));
                    position++;
                }
                appendOperationMap(tempList);
//                System.out.println("append + 1");
                start--;
            }
            end++;
        }
    }

    private void appendOperationMap (List<String> subOperatingList) {
        operationCount = operationCount + 1;
        if (operationDictionary.get(subOperatingList) != null)
            operationDictionary.put(subOperatingList, operationDictionary.get(subOperatingList) + 1);
        else
            operationDictionary.put(subOperatingList, 1L);
    }


    public void removeTooShortOperationList () {
        if (operationDictionary.isEmpty())
            return;
        for (List<String> temp: operationDictionary.keySet()) {
            if (temp.size() < minHotkeyLength){
                operationCount = operationCount - operationDictionary.get(temp);
                operationDictionary.put(temp, 0L);
            }

        }
    }

    public List<List<String>> getFrequentOperationList () {
        // Logic for Thread interrupted or not.
        if (Thread.currentThread().isInterrupted())
            return null;
        // (Core)
        // Generate the operation dictionary
        for (List<String> operationList : operationLists) {
            slideWindowSublist(operationList);
        }
        // remove Too Short Operation List
        removeTooShortOperationList();
        // Re-generate the frequent operation list.
//        double frequentPercentage = 0.001; //Config.getHotkeyAppearancePercentage();
//        int frequentTimes = (int) (frequentPercentage * operationCount);
        int frequentTimes = 10;

        operationLists = new ArrayList<>();
        for (List<String> key : operationDictionary.keySet()) {
            if (operationDictionary.get(key) >= frequentTimes)
                operationLists.add(key);
        }
        return operationLists;
    }

    public List<Map<List<String>, Long>> getFrequentOperationListWithCounting () {
        // Logic for Thread interrupted or not.
        if (Thread.currentThread().isInterrupted())
            return null;
        // (Core)
        // Generate the operation dictionary
        for (List<String> operationList : operationLists) {
            slideWindowSublist(operationList);
        }
        // remove Too Short Operation List
        removeTooShortOperationList();
        // Re-generate the frequent operation list.
//        double frequentPercentage = 0.001; //Config.getHotkeyAppearancePercentage();
//        int frequentTimes = (int) (frequentPercentage * operationCount);
        int frequentTimes = 10;

        List<Map<List<String>, Long>> result = new ArrayList<>();
        for (List<String> key : operationDictionary.keySet()) {
            if (operationDictionary.get(key) >= frequentTimes) {
                Map<List<String>, Long> recordPair = new HashMap<>(1);
                recordPair.put(key, operationDictionary.get(key));
                result.add(recordPair);
            }
        }
        return result;
    }

    public void reset () {
        operationDictionary.clear();
        operationLists.clear();
    }

}
