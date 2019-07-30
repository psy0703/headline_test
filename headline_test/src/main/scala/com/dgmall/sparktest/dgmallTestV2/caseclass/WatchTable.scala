package com.dgmall.sparktest.dgmallTestV2.caseclass

/**
  * @Author: Cedaris
  * @Date: 2019/7/17 18:15
  */
case class WatchTable(
                       distinct_id: String,
                       Time: String,
                       Event: String,
                       Type: String,
                       trace_id: String, //由推荐引擎生成：appid.场景id.方案id.分桶id
                       alg_match: String, //召回层策略，由推荐引擎生成
                       alg_rank: String, //排序层策略，由推荐引擎生成

                       rule: String, //规则层策略，由推荐引擎生成

                       order: String, //如果是推荐引导，取值推荐列表的展现次序

                       user_id: String, //用户ID

                       video_id: String, //视频ID

                       video_user_id: String, //视频作者ID

                       video_desc: String, //视频描述

                       watch_time_long: String, //观看视频时长

                       is_attention: String, //是否关注（0代表无、1代表有）

                       is_like: String, //是否点赞（0代表无、1代表有）

                       is_comment: String, //是否评论（0代表无、1代表有）

                       is_share_weixin: String, //是否分享到微信好友（0代表无、1代表有）

                       is_share_friendster: String, //是否分享到朋友圈（0代表无、1代表有）

                       is_share_qq: String, //是否分享到QQ好友（0代表无、1代表有）

                       is_save: String, //是否保存到相册（0代表无、1代表有）

                       is_get_red_packets: String, //是否领红包（0代表无、1代表有）

                       red_packets_sum: String, //红包金额

                       is_copy_site: String, //是否复制链接（0代表无、1代表有）

                       is_report: String, //是否举报（0代表无、1代表有）

                       report_content: String, //举报内容

                       is_not_interested: String, //is_not_interested	String	不感兴趣（0代表无、1代表有）

                       is_go_shop: String, //是否进店铺（0代表无、1代表有）

                       shop_id: String, //shop_id	String	店铺ID

                       shop_name: String //店铺名
                     ) {

}
/*
create table test_watch (
distinct_id  String,
                       `Time` String,
                       Event  String,
                       Type  String,
                       trace_id  String,
                       alg_match  String,
                       alg_rank  String,
                       rule  String,
                       bhv_amt  String,
                       user_id  String,
                       video_id  String,
                       video_user_id  String,
                       video_desc  String,
                       video_tag  String,
                       watch_time_long  String,
                       video_long  String,
                       music_name  String,
                       music_write  String,
                       video_topic  String,
                       video_address  String,
                       is_attention  String,
                       is_like  String,
                       is_comment  String,
                       is_share_weixin  String,
                       is_share_friendster  String,
                       is_share_qq  String,
                       is_save  String,
                       is_get_red_packets  String,
                       red_packets_sum  String,
                       is_copy_site  String,
                       is_report  String,
                       report_content  String,
                       is_not_interested  String,
                       is_go_shop  String,
                       shop_id  String,
                       shop_name  String
)PARTITIONED BY (`log_time` string) --指定分区
row format delimited fields terminated by '\t'
 */
