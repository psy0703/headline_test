package com.dgmall.log.bean;

/**
 * 公共字段
 * @Author: Cedaris
 * @Date: 2019/7/17 11:20
 */
public class AppBaseField {
    private String distinct_id; //设备标识码
/*    private String Event;   //事件名
    private String Type;    //事件类型*/
    private String Time ;   //事件发生的实际时间

    public String getDistinct_id() {
        return distinct_id;
    }

    public void setDistinct_id(String distinct_id) {
        this.distinct_id = distinct_id;
    }

    /*public String getEvent() {
        return Event;
    }

    public void setEvent(String event) {
        Event = event;
    }

    public String getType() {
        return Type;
    }

    public void setType(String type) {
        Type = type;
    }*/

    public String getTime() {
        return Time;
    }

    public void setTime(String time) {
        Time = time;
    }
}
/*
distinct_id 说明:
Android	默认使UUID（例如：550e8400-e29b-41d4-a716-446655440000），App 卸载重装 UUID 会变，为了保证设备 ID 不变，可以通过配置使用 AndroidId（例如：774d56d682e549c），如果 AndroidId 获取不到则获取随机的 UUID。
iOS	默认情况下IDFV（例如：1E2DFA10-236A-47UD-6641-AB1FC4E6483F），如果 IDFV 获取失败，则使用随机的 UUID（例如：550e8400-e29b-41d4-a716-446655440000）
JS	默认情况下使用 cookie_id（例如：15ffdb0a3f898-02045d1cb7be78-31126a5d-250125-15ffdb0a3fa40a）存贮在浏览器的 cookie 中，规则为五段不同含义的字段拼接而成来保证唯一性，其中包括两段时间戳，一段屏幕宽高，一段随机数，一段 UA 值。
微信小程序

 */
