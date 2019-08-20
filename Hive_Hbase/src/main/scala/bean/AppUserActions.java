package bean;

/**
 * @Author: Cedaris
 * @Date: 2019/8/19 10:38
 */
public class AppUserActions {
    private String user_id;
    private String time;
    private String watch_last_time;
    private Double timesincelastwatch;
    private Double timesincelastwatchsqrt;
    private Double timesincelastwatchsquare;
   /* private ArrayList<String> behaviorvids;
    private ArrayList<String> behavioraids;
    private ArrayList<String> behaviorcids;
    private ArrayList<String> behaviorc1ids;
    private ArrayList<String> behaviortokens;
    private ArrayList<String> cate1_prefer;
    private ArrayList<Double> weights_cate1_prefer;
    private ArrayList<String> cate2_prefer;
    private ArrayList<Double> weights_cate2_prefer;*/

    private String behaviorvids;
    private String behavioraids;
    private String behaviorcids;
    private String behaviorc1ids;
    private String behaviortokens;
    private String cate1_prefer;
    private String weights_cate1_prefer;
    private String cate2_prefer;
    private String weights_cate2_prefer;

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getWatch_last_time() {
        return watch_last_time;
    }

    public void setWatch_last_time(String watch_last_time) {
        this.watch_last_time = watch_last_time;
    }

    public Double getTimesincelastwatch() {
        return timesincelastwatch;
    }

    public void setTimesincelastwatch(Double timesincelastwatch) {
        this.timesincelastwatch = timesincelastwatch;
    }

    public Double getTimesincelastwatchsqrt() {
        return timesincelastwatchsqrt;
    }

    public void setTimesincelastwatchsqrt(Double timesincelastwatchsqrt) {
        this.timesincelastwatchsqrt = timesincelastwatchsqrt;
    }

    public Double getTimesincelastwatchsquare() {
        return timesincelastwatchsquare;
    }

    public void setTimesincelastwatchsquare(Double timesincelastwatchsquare) {
        this.timesincelastwatchsquare = timesincelastwatchsquare;
    }

    public String getBehaviorvids() {
        return behaviorvids;
    }

    public void setBehaviorvids(String behaviorvids) {
        this.behaviorvids = behaviorvids;
    }

    public String getBehavioraids() {
        return behavioraids;
    }

    public void setBehavioraids(String behavioraids) {
        this.behavioraids = behavioraids;
    }

    public String getBehaviorcids() {
        return behaviorcids;
    }

    public void setBehaviorcids(String behaviorcids) {
        this.behaviorcids = behaviorcids;
    }

    public String getBehaviorc1ids() {
        return behaviorc1ids;
    }

    public void setBehaviorc1ids(String behaviorc1ids) {
        this.behaviorc1ids = behaviorc1ids;
    }

    public String getBehaviortokens() {
        return behaviortokens;
    }

    public void setBehaviortokens(String behaviortokens) {
        this.behaviortokens = behaviortokens;
    }

    public String getCate1_prefer() {
        return cate1_prefer;
    }

    public void setCate1_prefer(String cate1_prefer) {
        this.cate1_prefer = cate1_prefer;
    }

    public String getWeights_cate1_prefer() {
        return weights_cate1_prefer;
    }

    public void setWeights_cate1_prefer(String weights_cate1_prefer) {
        this.weights_cate1_prefer = weights_cate1_prefer;
    }

    public String getCate2_prefer() {
        return cate2_prefer;
    }

    public void setCate2_prefer(String cate2_prefer) {
        this.cate2_prefer = cate2_prefer;
    }

    public String getWeights_cate2_prefer() {
        return weights_cate2_prefer;
    }

    public void setWeights_cate2_prefer(String weights_cate2_prefer) {
        this.weights_cate2_prefer = weights_cate2_prefer;
    }
}
