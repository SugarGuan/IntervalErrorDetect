package core.learn;

import util.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class HotkeyFinder {
    private int maxHotkeyLength = 20;
    private int minHotkeyLength = 3;
    private Long operationCount = 0L;
    private Map<List<String >, Long> operationDictionary = new HashMap<>();
    private List<List<String> > operationLists = new ArrayList<>();

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
            System.out.println("Slide window sublist ERROR : operation list empty");
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
        // (Core)
        // Generate the operation dictionary
        for (List<String> operationList : operationLists) {
            slideWindowSublist(operationList);
        }
        // remove Too Short Operation List
        removeTooShortOperationList();
        // Re-generate the frequent operation list.
        double frequentPercentage = 0.001; //Config.getHotkeyAppearancePercentage();
        int frequentTimes = (int) (frequentPercentage * operationCount);
        operationLists = new ArrayList<>();
        for (List<String> key : operationDictionary.keySet()) {
            if (operationDictionary.get(key) >= frequentTimes)
                operationLists.add(key);
        }
        return operationLists;
    }
}
