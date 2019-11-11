package core.learn;

import util.Time;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class Dict {
    private static Map<String , Long> dict = new HashMap<>();
    private static Map<List<String > , Long> dict2 = new HashMap<>();

    public static void append(String str) {
        if (dict.get(str) != null) {
            dict.put(str, dict.get(str) + 1);
        }
        else
            dict.put(str, 1L);
    }

    public static void append(List<String> list) {
        if (dict2.get(list) != null) {
            dict2.put(list, dict2.get(list) + 1);
        }
        else
            dict2.put(list, 1L);
    }

    public static void listst(List<String> list) {
        int size = list.size();
        int locate = 0;
        int start = 0;
        int end = 0;
//        StringBuffer tempBuffer = new StringBuffer();
        List<String> tempList = new ArrayList<>();

        while ( end < size ) {
            start = end;
            while (start > -1 && start > (end - 5)) {
                locate = start;
//                tempBuffer.setLength(0);
                tempList = new ArrayList<>();
                while (locate <= end) {
                    tempList.add(list.get(locate));
//                    tempBuffer.append(list.get(locate));
                    locate++;
                }
//                append(tempBuffer.toString());
                append(tempList);
                start--;
            }
            end++;
        }
    }

    public static void main(String[] args) {
        Long jobStartTime = Time.now();
        List<String> list1 = new ArrayList<>();
        List<String> list2 = new ArrayList<>();
        int f = 5;
        {

            list1.add("c");
            list1.add("a");
            list1.add("b");
            list1.add("c");
            list1.add("a");
            list1.add("b");
            list1.add("c");
            list1.add("a");
//            list1.add("a");
//            list1.add("b");
//            list1.add("c");
//            list1.add("d");
//            list1.add("a");
//            list1.add("b");
//            list1.add("c");
        }
        {
            list2.add("a");
            list2.add("c");
            list2.add("c");
            list2.add("a");
            list2.add("b");
            list2.add("b");
            list2.add("a");
            list2.add("b");
            list2.add("c");
            list2.add("c");
        }
        int i = 0;
        while (i < 10) {
            listst(list1);
//            listst(list2);
            i++;
        }

        for (List<String> temp : dict2.keySet()) {
            if (temp.size() < 3) {
                dict2.put(temp, 0L);
            }
        }

        Long max = 0L;
        StringBuilder sbn = new StringBuilder();
        for (List<String> temp : dict2.keySet()){
            if (dict2.get(temp).equals(max)) {
                sbn.append(", ");
                sbn.append(temp);
                continue;
            }
            if (dict2.get(temp) > max) {
                max = dict2.get(temp);
                sbn.setLength(0);
                sbn.append(temp);
            }
        }
        List<String> searchPattern1 = new ArrayList<>();
        searchPattern1.add("b");
        searchPattern1.add("c");
        searchPattern1.add("a");

        List<String> searchPattern2 = new ArrayList<>();
        searchPattern2.add("a");
        searchPattern2.add("b");
        searchPattern2.add("c");

        List<String> searchPattern3 = new ArrayList<>();
        searchPattern3.add("a");
        searchPattern3.add("b");
        searchPattern3.add("c");
        searchPattern3.add("a");


        System.out.println(searchPattern1);
        System.out.println(dict2.get(searchPattern1));
        System.out.println(searchPattern2);
        System.out.println(dict2.get(searchPattern2));
        System.out.println(searchPattern3);
        System.out.println(dict2.get(searchPattern3));
        System.out.println("Max appearance word is : " + sbn.toString());
        System.out.println("Appear time max is : " + max);
        Long jobFinishTime = Time.now();
        System.out.println("Execute Duration : " + Time.timeFormatEnglish(jobFinishTime - jobStartTime));
    }

}
