package dao.websocket;

public class Model {
    /**
     * dao.websocket.Model 类
     * 从websocket接收到的是json格式的文件，需要对其进行json格式的解析。
     * 该类为解析类org.alibaba.FastJson类提供数据模板。
     */
    private String option;
    private String state;
    public void setOption(String option) {
        this.option = option;
    }
    public String getOption() {
        return this.option;
    }
    public void setState(String state) {
        this.state = state;
    }
    public String getState() {
        return this.state;
    }
}
