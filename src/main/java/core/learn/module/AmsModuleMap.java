package core.learn.module;

import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AmsModuleMap implements Function<Iterable<Map<String, Object>>, List<String>> {
    public List<String> operationList;
    @Override
    public List<String> call (Iterable<Map<String, Object>> maps) throws Exception {
        operationList = new ArrayList<>();
        for (Map<String, Object> map: maps) {
            operationList.add((String) map.get("i_cmd"));
        }
        return operationList;
    }

}