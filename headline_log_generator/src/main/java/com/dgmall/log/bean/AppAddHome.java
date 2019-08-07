package com.dgmall.log.bean;

/**
 * 用户设置家乡地址
 * @Author: Cedaris
 * @Date: 2019/7/17 11:15
 */
public class AppAddHome {
    private String user_id;           //用户id
    private String hometown_province; //家乡所在省
    private String hometown_city;       //家乡所在市
    private String hometown_area;       //家乡所在区
    private String hometown_town;       //家乡所在镇

    private String GPS_province;        //定位所在省
    private String GPS_city;        //定位所在市
    private String GPS_area;        //定位所在区
    private String GPS_town;        //定位所在镇

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getHometown_province() {
        return hometown_province;
    }

    public void setHometown_province(String hometown_province) {
        this.hometown_province = hometown_province;
    }

    public String getHometown_city() {
        return hometown_city;
    }

    public void setHometown_city(String hometown_city) {
        this.hometown_city = hometown_city;
    }

    public String getHometown_area() {
        return hometown_area;
    }

    public void setHometown_area(String hometown_area) {
        this.hometown_area = hometown_area;
    }

    public String getHometown_town() {
        return hometown_town;
    }

    public void setHometown_town(String hometown_town) {
        this.hometown_town = hometown_town;
    }

    public String getGPS_province() {
        return GPS_province;
    }

    public void setGPS_province(String GPS_province) {
        this.GPS_province = GPS_province;
    }

    public String getGPS_city() {
        return GPS_city;
    }

    public void setGPS_city(String GPS_city) {
        this.GPS_city = GPS_city;
    }

    public String getGPS_area() {
        return GPS_area;
    }

    public void setGPS_area(String GPS_area) {
        this.GPS_area = GPS_area;
    }

    public String getGPS_town() {
        return GPS_town;
    }

    public void setGPS_town(String GPS_town) {
        this.GPS_town = GPS_town;
    }
}
