package bean;

/**
 * @Author: Cedaris
 * @Date: 2019/8/19 10:46
 */
public class AppUserInfo {
    private String user_id;
    private Double sum_play_long;
    private Long sum_play_times;
    private Long play_long_rank;
    private Long play_times_rank;
    private String value_type;
    private String frequence_type;

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public Double getSum_play_long() {
        return sum_play_long;
    }

    public void setSum_play_long(Double sum_play_long) {
        this.sum_play_long = sum_play_long;
    }

    public Long getSum_play_times() {
        return sum_play_times;
    }

    public void setSum_play_times(Long sum_play_times) {
        this.sum_play_times = sum_play_times;
    }

    public Long getPlay_long_rank() {
        return play_long_rank;
    }

    public void setPlay_long_rank(Long play_long_rank) {
        this.play_long_rank = play_long_rank;
    }

    public Long getPlay_times_rank() {
        return play_times_rank;
    }

    public void setPlay_times_rank(Long play_times_rank) {
        this.play_times_rank = play_times_rank;
    }

    public String getValue_type() {
        return value_type;
    }

    public void setValue_type(String value_type) {
        this.value_type = value_type;
    }

    public String getFrequence_type() {
        return frequence_type;
    }

    public void setFrequence_type(String frequence_type) {
        this.frequence_type = frequence_type;
    }
}
