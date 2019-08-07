package com.dgmall.log.bean;

/**
 * 视频列表点击日志
 * @Author: Cedaris
 * @Date: 2019/7/17 11:16
 */
public class AppClick {
    private String  order;     //如果是推荐引导，取值推荐列表的展现次序
    private String user_id;     //用户id
    private String video_id;    //视频id
    private String trace_id;    //由推荐引擎生成：appid.场景id.方案id.分桶id

    public String getOrder() {
        return order;
    }

    public void setOrder(String order) {
        this.order = order;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getVideo_id() {
        return video_id;
    }

    public void setVideo_id(String video_id) {
        this.video_id = video_id;
    }

    public String getTrace_id() {
        return trace_id;
    }

    public void setTrace_id(String trace_id) {
        this.trace_id = trace_id;
    }
}
