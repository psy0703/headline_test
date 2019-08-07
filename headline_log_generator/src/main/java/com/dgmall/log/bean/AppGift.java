package com.dgmall.log.bean;

/**
 * @Author: Cedaris
 * @Date: 2019/7/17 11:17
 */
public class AppGift {

    private String content;     //门店名称
    private String user_id;     //用户id
    private String video_id;    //视频id
    private String trace_id;    //由推荐引擎生成：appid.场景id.方案id.分桶id

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
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
