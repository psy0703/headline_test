package com.dgmall.sparktest.dgmallTestV2.caseclass

/**
  * @Author: Cedaris
  * @Date: 2019/8/15 14:54
  */
case class UserActionsSummay (
                             user_id:String,
                             time:String,
                             watch_last_time:String,
                             timesincelastwatch:Double,
                             timesincelastwatchsqrt:Double,
                             timesincelastwatchsquare:Double,
                             behaviorvids:Array[String],
                             behavioraids:Array[String],
                             behaviorcids:Array[String],
                             behaviorc1ids:Array[String],
                             behaviortokens:Array[String],
                             cate1_prefer:Array[String],
                             weights_cate1_prefer:Array[Double],
                             cate2_prefer:Array[String],
                             weights_cate2_prefer:Array[Double]
                             ){

  override def toString: String = {
    user_id + "\t" + time + "\t" + watch_last_time +
      "\t" +  timesincelastwatch+  "\t" +  timesincelastwatchsqrt + "\t" +
      timesincelastwatchsquare+ "\t" +  behaviorvids + "\t" + behavioraids +
      "\t" +  behaviorcids+"\t" + behaviorc1ids +"\t" +  behaviortokens +"\t" +
      cate1_prefer+ "\t" +  weights_cate1_prefer + "\t" + cate2_prefer + "\t"+ weights_cate2_prefer
  }
}
