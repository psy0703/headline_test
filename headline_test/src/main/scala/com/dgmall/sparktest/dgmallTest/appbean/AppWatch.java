package com.dgmall.sparktest.dgmallTest.appbean;

/**
 * 用户观看视频
 * @Author: Cedaris
 * @Date: 2019/7/17 11:16
 */
public class AppWatch {
    private String trace_id;    //由推荐引擎生成：appid.场景id.方案id.分桶id
    private String alg_match;   //召回层策略，由推荐引擎生成
    private String alg_rank;    //排序层策略，由推荐引擎生成
    private String rule;        //规则层策略，由推荐引擎生成
//    private String action;      //具体行为
    private String  bhv_amt;    //如果是推荐引导，取值推荐列表的展现次序
    private String user_id;     //用户ID
    private String video_id;    //视频ID
    private String video_user_id;//视频作者ID
    private String video_desc;   //视频描述
    private String video_tag;    //视频标签
    private String watch_time_long;//观看视频时长
    private String video_long;      //视频时长
    private String music_name;      //音乐名字
    private String music_write;     //音乐作者
    private String video_topic;     //视频主题
    private String video_address;   //视频地址
    private String is_attention;    //是否关注（0代表无、1代表有）
    private String is_like;         //是否点赞（0代表无、1代表有）
    private String is_comment;      //是否评论（0代表无、1代表有）
    private String is_share_weixin; //是否分享到微信好友（0代表无、1代表有）
    private String is_share_friendster;//是否分享到朋友圈（0代表无、1代表有）
    private String is_share_qq;     //是否分享到QQ好友（0代表无、1代表有）
    private String is_save;         //是否保存到相册（0代表无、1代表有）
    private String is_get_red_packets;//是否领红包（0代表无、1代表有）
    private String red_packets_sum;     //红包金额
    private String is_copy_site;        //是否复制链接（0代表无、1代表有）
    private String is_report;           //是否举报（0代表无、1代表有）
    private String report_content;      //举报内容
    private String is_not_interested;   //is_not_interested	String	不感兴趣（0代表无、1代表有）
    private String is_go_shop;          //是否进店铺（0代表无、1代表有）
    private String shop_id;             //shop_id	String	店铺ID
    private String shop_name;           //店铺名

    public String getTrace_id() {
        return trace_id;
    }

    public void setTrace_id(String trace_id) {
        this.trace_id = trace_id;
    }

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

  /*  public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }*/

    public String getBhv_amt() {
        return bhv_amt;
    }

    public void setBhv_amt(String bhv_amt) {
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

    public String getVideo_user_id() {
        return video_user_id;
    }

    public void setVideo_user_id(String video_user_id) {
        this.video_user_id = video_user_id;
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

    public String getWatch_time_long() {
        return watch_time_long;
    }

    public void setWatch_time_long(String watch_time_long) {
        this.watch_time_long = watch_time_long;
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

    public String getIs_attention() {
        return is_attention;
    }

    public void setIs_attention(String is_attention) {
        this.is_attention = is_attention;
    }

    public String getIs_like() {
        return is_like;
    }

    public void setIs_like(String is_like) {
        this.is_like = is_like;
    }

    public String getIs_comment() {
        return is_comment;
    }

    public void setIs_comment(String is_comment) {
        this.is_comment = is_comment;
    }

    public String getIs_share_weixin() {
        return is_share_weixin;
    }

    public void setIs_share_weixin(String is_share_weixin) {
        this.is_share_weixin = is_share_weixin;
    }

    public String getIs_share_friendster() {
        return is_share_friendster;
    }

    public void setIs_share_friendster(String is_share_friendster) {
        this.is_share_friendster = is_share_friendster;
    }

    public String getIs_share_qq() {
        return is_share_qq;
    }

    public void setIs_share_qq(String is_share_qq) {
        this.is_share_qq = is_share_qq;
    }

    public String getIs_save() {
        return is_save;
    }

    public void setIs_save(String is_save) {
        this.is_save = is_save;
    }

    public String getIs_get_red_packets() {
        return is_get_red_packets;
    }

    public void setIs_get_red_packets(String is_get_red_packets) {
        this.is_get_red_packets = is_get_red_packets;
    }

    public String getRed_packets_sum() {
        return red_packets_sum;
    }

    public void setRed_packets_sum(String red_packets_sum) {
        this.red_packets_sum = red_packets_sum;
    }

    public String getIs_copy_site() {
        return is_copy_site;
    }

    public void setIs_copy_site(String is_copy_site) {
        this.is_copy_site = is_copy_site;
    }

    public String getIs_report() {
        return is_report;
    }

    public void setIs_report(String is_report) {
        this.is_report = is_report;
    }

    public String getReport_content() {
        return report_content;
    }

    public void setReport_content(String report_content) {
        this.report_content = report_content;
    }

    public String getIs_not_interested() {
        return is_not_interested;
    }

    public void setIs_not_interested(String is_not_interested) {
        this.is_not_interested = is_not_interested;
    }

    public String getIs_go_shop() {
        return is_go_shop;
    }

    public void setIs_go_shop(String is_go_shop) {
        this.is_go_shop = is_go_shop;
    }

    public String getShop_id() {
        return shop_id;
    }

    public void setShop_id(String shop_id) {
        this.shop_id = shop_id;
    }

    public String getShop_name() {
        return shop_name;
    }

    public void setShop_name(String shop_name) {
        this.shop_name = shop_name;
    }
}
