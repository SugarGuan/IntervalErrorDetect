package core.learn.field;

import java.util.ArrayList;
import java.util.List;

public class Field {

    private static List<List<String>>  operationList = new ArrayList<>();

    public static void append (List<String> strList) {operationList.add(strList); };

    public static List<List<String>> getStrList() { return operationList; };

    public static void reset() {
        operationList = new ArrayList<>();
    }

}
