package com.dgmall.sparktest.dgmallTestV2

import com.alibaba.fastjson.JSON
import com.dgmall.sparktest.dgmallTestV2.bean.AppModelFeatures

/**
  * @Author: Cedaris
  * @Date: 2019/8/19 11:07
  */
object DemoTest {

  def main(args: Array[String]): Unit = {

    val str = "{\"value_type\":\"L03\",\"uv_ctr_1d\":\"0.667\",\"play_times_2w\":\"1.0\",\"videoId\":\"00001\"," +
      "\"uv_ctr_1m\":\"0.4\",\"exampleAge\":\"0\",\"catePrefer\":\"0\",\"phoneBrand\":\"HW\",\"behaviorC1ids\":\"WrappedArray(93, 0, 0, 0, 0, 0, 0)\",\"cate2Id\":\"101\",\"weights_cate1_prefer\":\"WrappedArray(1.0)\",\"behaviorAids\":\"WrappedArray(83483, 0, 0, 0, 0, 0, 0)\",\"play_long_2w\":\"85.0\",\"behaviorTokens\":\"WrappedArray(炭你项矮狐, 训穗特医肄, 哇瘁蛾高淌, 乞酱刽伍基, 萎曼职般疾, 挑秤邮遭累, 铂惦卸恤眶, 摹刁莹悔午, 余铣峦浦芍, 薯萧靳捂文)\",\"uv_ctr_1w\":\"0.4\",\"click\":\"1\",\"cate2_prefer\":\"WrappedArray(93)\",\"matchScore\":\"0\",\"frequence_type\":\"L03\",\"ctr_1d\":\"0.667\",\"cateId\":\"22\",\"position\":\"1\",\"cate2Prefer\":\"0\",\"ctr_1m\":\"0.4\",\"timeSinceLastWatch\":\"156256.0\",\"ctr_1w\":\"0.4\",\"city\":\"深圳\",\"play_long_1w\":\"85.0\",\"uv_ctr_2w\":\"0.4\",\"timeSinceLastWatchSquare\":\"2.4415937536E10\",\"audience_id\":\"13763\",\"timeSinceLastWatchSqrt\":\"395.29\",\"play_long_1m\":\"85.0\",\"phoneResolution\":\"1920*1080\",\"play_long_1d\":\"0.0\",\"play_times_1d\":\"0.0\",\"hour\":\"0\",\"cate1Id\":\"66\",\"weights_cate2_prefer\":\"WrappedArray(1.0)\",\"play_times_1m\":\"1.0\",\"ctr_2w\":\"0.4\",\"item_id\":\"94620\",\"behaviorVids\":\"WrappedArray(76907, 15441, 43287, 61437, 15640, 56980, 29633)\",\"play_times_1w\":\"1.0\",\"triggerNum\":\"0\",\"triggerRank\":\"0\",\"authorId\":\"9999\",\"cate1_prefer\":\"WrappedArray(25)\",\"behaviorCids\":\"WrappedArray(25, 0, 0, 0, 0, 0, 0)\",\"popScore\":\"0\",\"authorPrefer\":\"0\"}"
    val features: AppModelFeatures = JSON.parseObject(str,classOf[AppModelFeatures])

    println(features.getAudience_id)
  }
}

