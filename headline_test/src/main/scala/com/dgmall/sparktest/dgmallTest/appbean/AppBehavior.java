package com.dgmall.sparktest.dgmallTest.appbean;

/**
 * 详情-浏览点击日志——浏览与点击并一起了
 * @Author: Cedaris
 * @Date: 2019/7/17 11:17
 */
public class AppBehavior {
    private String alg_match; //由推荐引擎生成：如editor_recommend
    private String alg_rank;    //由推荐引擎生成：如editor_recommend
    private String rule;        //由推荐引擎生成：如rule1
    private String bhv_type;    //取值 “detail_click”为点击；取值“detail_view”为浏览

    public String getBhv_type() {
        return bhv_type;
    }

    public void setBhv_type(String bhv_type) {
        this.bhv_type = bhv_type;
    }

    private Float  bhv_amt;     //如果是推荐引导，取值推荐列表的展现次序
    private String user_id;     //用户id
    private String video_id;    //视频id
    private String trace_id;    //由推荐引擎生成：appid.场景id.方案id.分桶id

    public String getAlg_match() {
        return alg_match;
    }

    public void setAlg_match(String alg_match) {
        this.alg_match = alg_match;
    }

    public String getAlg_rank() {
        return alg_rank;
    }

    public void setAlg_rank(String alg_rank) {
        this.alg_rank = alg_rank;
    }

    public String getRule() {
        return rule;
    }

    public void setRule(String rule) {
        this.rule = rule;
    }

    public Float getBhv_amt() {
        return bhv_amt;
    }

    public void setBhv_amt(Float bhv_amt) {
        this.bhv_amt = bhv_amt;
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
