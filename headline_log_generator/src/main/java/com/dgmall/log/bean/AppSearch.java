package com.dgmall.log.bean;

/**
 * 搜索点击日志
 * @Author: Cedaris
 * @Date: 2019/7/17 11:17
 */
public class AppSearch {
    private String search_content; //搜索词
    private String user_id;         //用户id

    public String getSearch_content() {
        return search_content;
    }

    public void setSearch_content(String search_content) {
        this.search_content = search_content;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }
}
