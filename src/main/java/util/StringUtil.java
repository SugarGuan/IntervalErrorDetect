package util;

public class StringUtil {
    /**
     * StringUtil 类
     * 提供数值-文本转换方法
     */

    /**
     * trans()
     * 用于将不同类型对象转换成适合频率发现算法使用的String格式
     * @param obj 数据对象
     * @return String类型的文本数据对象
     */
    public static String trans(Object obj){
        if (obj == null)
            return "";
        if (obj instanceof String)
            return (String) obj;
        if (obj instanceof Long)
            return Long.toString((Long) obj);
        if (obj instanceof Integer)
            return Integer.toString((Integer) obj);
        return obj.toString();
    }
}

