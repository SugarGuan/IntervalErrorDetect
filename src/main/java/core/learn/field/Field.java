package core.learn.field;

import java.util.ArrayList;
import java.util.List;

public class Field {

    private static List<List<String>>  fieldList = new ArrayList<>();

    public static void append (List<String> strList) {fieldList.add(strList); };

    public static List<List<String>> getStrList() { return fieldList; };

    public static void reset() {
        fieldList = new ArrayList<>();
    }

}
