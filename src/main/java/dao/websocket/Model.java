package dao.websocket;

public class Model {
    /**
     * dao.websocket.Model 类
     * 用于给Socket类中解析Json的org.alibaba.FastJson类提供数据模板类
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
