package core.learn.field;

import java.util.ArrayList;
import java.util.List;

public class Field {

    static List<List<String>>  operationList = new ArrayList<>();

    static void append (List<String> strList) {operationList.add(strList); };

    static List<List<String>> getStrList() { return operationList; };

    static void reset() {
        operationList = new ArrayList<>();
    }

}
