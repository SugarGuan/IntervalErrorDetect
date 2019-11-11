package core.learn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class HotkeyFinder {
    private int maxHotkeyLength = 20;
    private int minHotkeyLength = 3;
    private int listCounter = 0;
    private Map<List<String >, Long> hotkeyDictionary = new HashMap<>();

    public void setMaxHotkeyLength (int maxHotkeyLength) {
        this.maxHotkeyLength = maxHotkeyLength;
    }

    public int getMaxHotkeyLength () {
        return maxHotkeyLength;
    }

    public void setMinHotkeyLength (int minHotkeyLength) {
        this.minHotkeyLength = minHotkeyLength;
    }

    public int getMinHotkeyLength () {
        return minHotkeyLength;
    }

    public void setDefaultMaxHotkeyLength () {
        setMaxHotkeyLength(20);
    }

    public void setDefaultMinHotkeyLength () {
        setMinHotkeyLength(3);
    }

    public void append (List<String> subOperatingList) {
        listCounter++;
        if (hotkeyDictionary.get(subOperatingList) != null)
            hotkeyDictionary.put(subOperatingList, hotkeyDictionary.get(subOperatingList) + 1);
        else
            hotkeyDictionary.put(subOperatingList, 1L);
    }

    public void slidingWindowAction (List<String> operationList) {
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
                append(tempList);
                start--;
            }
            end++;
        }
    }

    public void removeTooShortOperationList () {
        if (hotkeyDictionary.isEmpty())
            return;
        for (List<String> temp: hotkeyDictionary.keySet()) {
            if (temp.size() < minHotkeyLength)
                hotkeyDictionary.put(temp, 0L);
        }
    }

    public List<List<String> > getMaxAppearanceNoLimition (double percentage) {
        if (hotkeyDictionary.isEmpty())
            return null;
        removeTooShortOperationList();
        List<List<String> > result = new ArrayList<>();
        if (percentage > 1)
            percentage = 1;
        if (percentage < 0)
            percentage = 0;
        int appearanceTimesLowerLimit = (int) (percentage * listCounter);
        for (List<String> key : hotkeyDictionary.keySet()) {
            if (hotkeyDictionary.get(key) >= appearanceTimesLowerLimit)
                result.add(key);
        }
        return result;
    }

    public List<List<String> > getMaxAppearance (double percentage) {
        List<List<String> > resultTemp = getMaxAppearanceNoLimition(percentage);
        List<List<String> > result = new ArrayList<>();
        for (List<String> key: resultTemp) {
            if (key.size() >= minHotkeyLength)
                result.add(key);
        }
        return result;
    }

}
