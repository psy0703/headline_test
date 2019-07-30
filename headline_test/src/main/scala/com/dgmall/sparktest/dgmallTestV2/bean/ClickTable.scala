package com.dgmall.sparktest.dgmallTestV2.bean

/**
  * @Author: Cedaris
  * @Date: 2019/7/23 10:54
  */
case class ClickTable (
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
                      ){

}
