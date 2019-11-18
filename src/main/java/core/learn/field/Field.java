package core.learn.field;

import java.util.ArrayList;
import java.util.List;

public class Field {

    /**
     * Field类
     * 所有 *Field 类的dao模板类。该类保存对应field，
     * 提供追加、获取和重置方法.
     */

    private static List<List<String>>  fieldList = new ArrayList<>();

    public static void append (List<String> strList) {fieldList.add(strList); };

    public static List<List<String>> getStrList() { return fieldList; };

    public static void reset() {
        fieldList = new ArrayList<>();
    }

}
