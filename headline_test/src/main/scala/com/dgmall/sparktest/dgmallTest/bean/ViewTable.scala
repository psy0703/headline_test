package com.dgmall.sparktest.dgmallTest.bean

/**
  * @Author: Cedaris
  * @Date: 2019/7/17 16:57
  */
case class ViewTable(
                      distinct_id: String,
                      Time: String,
                      Event: String,
                      Type: String,
                      alg_match: String, //由推荐引擎生成：如editor_recommend
                      alg_rank: String,
                      rule: String, //由推荐引擎生成：如rule1
                      user_id: String, //用户id
                      video_id: String, //视频id
                      trace_id: String //由推荐引擎生成：appid.场景id.方案id.分桶id
                    ) {}

/**
create table test_view (
distinct_id  String,
`Time` String,
Event  String,
Type  String,
alg_match  String,
alg_rank  String,
rule  String,
user_id string,
video_id string,
trace_id string
)PARTITIONED BY (`log_time` string) --指定分区
row format delimited fields terminated by '\t'
  */