package bean;

/**
 * 登陆
 * @Author: Cedaris
 * @Date: 2019/7/17 11:15
 */
public class AppLogin {
    private String user_id;    //用户id
    private String login_type; //登陆类型 0 password_login/ 1 message_login/ 2 weixin_login
    private String user_phone; //用户手机号码
    private String app_version; //app版本
    private String ip_address;  //IP地址
    private String gps;         //GPS经度纬度
    private String user_agent;  //手机agent
    private String screen_width;//手机屏幕宽度
    private String screen_heigh;//手机屏幕高度

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getLogin_type() {
        return login_type;
    }

    public void setLogin_type(String login_type) {
        this.login_type = login_type;
    }

    public String getUser_phone() {
        return user_phone;
    }

    public void setUser_phone(String user_phone) {
        this.user_phone = user_phone;
    }

    public String getApp_version() {
        return app_version;
    }

    public void setApp_version(String app_version) {
        this.app_version = app_version;
    }

    public String getIp_address() {
        return ip_address;
    }

    public void setIp_address(String ip_address) {
        this.ip_address = ip_address;
    }

    public String getGps() {
        return gps;
    }

    public void setGps(String gps) {
        this.gps = gps;
    }

    public String getUser_agent() {
        return user_agent;
    }

    public void setUser_agent(String user_agent) {
        this.user_agent = user_agent;
    }

    public String getScreen_width() {
        return screen_width;
    }

    public void setScreen_width(String screen_width) {
        this.screen_width = screen_width;
    }

    public String getScreen_heigh() {
        return screen_heigh;
    }

    public void setScreen_heigh(String screen_heigh) {
        this.screen_heigh = screen_heigh;
    }
}
