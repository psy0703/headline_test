package com.dgmall.sparktest.dgmallTestV2.bean;

/**
 * 用户发布视频
 * @Author: Cedaris
 * @Date: 2019/7/17 11:15
 */
public class AppReleaseVideo {
    private String user_id;     //用户id
    private String video_id;    //视频id
    private String video_desc;  //视频描述
    private String video_tag;   //视频类型
    private String video_child_tag;   //视频类型
    private String video_long;  //视频时长
    private String music_name;  //配乐名字
    private String music_write; //配乐作者
    private String video_topic; //视频话题
    private String video_address;   //视频地址

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

    public String getVideo_desc() {
        return video_desc;
    }

    public void setVideo_desc(String video_desc) {
        this.video_desc = video_desc;
    }

    public String getVideo_tag() {
        return video_tag;
    }

    public void setVideo_tag(String video_tag) {
        this.video_tag = video_tag;
    }

    public String getVideo_long() {
        return video_long;
    }

    public void setVideo_long(String video_long) {
        this.video_long = video_long;
    }

    public String getMusic_name() {
        return music_name;
    }

    public String getVideo_child_tag() {
        return video_child_tag;
    }

    public void setVideo_child_tag(String video_child_tag) {
        this.video_child_tag = video_child_tag;
    }

    public void setMusic_name(String music_name) {
        this.music_name = music_name;
    }

    public String getMusic_write() {
        return music_write;
    }

    public void setMusic_write(String music_write) {
        this.music_write = music_write;
    }

    public String getVideo_topic() {
        return video_topic;
    }

    public void setVideo_topic(String video_topic) {
        this.video_topic = video_topic;
    }

    public String getVideo_address() {
        return video_address;
    }

    public void setVideo_address(String video_address) {
        this.video_address = video_address;
    }
}
