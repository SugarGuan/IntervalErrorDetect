package dao.websocket;

public class Model {
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
