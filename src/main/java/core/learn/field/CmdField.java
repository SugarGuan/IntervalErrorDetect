package core.learn.field;

import java.util.ArrayList;
import java.util.List;

public class CmdField extends Field{
    private static List<List<String>> fieldList = new ArrayList<>();

    public static void append (List<String> strList) {
        // 每个list都是一个字段在会话中的list。每个字段都是string，所以每个会话都是List<String>
        fieldList.add(strList);
    }

    public static List<List<String>> getStrList () {
        return fieldList;
    }

    public static void reset() {
        fieldList = new ArrayList<>();
    }

}
