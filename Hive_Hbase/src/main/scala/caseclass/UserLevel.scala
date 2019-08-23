package caseclass

/**
  * @Author: Cedaris
  * @Date: 2019/8/14 14:29
  */
case class UserLevel(
                      user_id:String,
                      sum_play_long:Double,
                      sum_play_times:Long,
                      play_long_rank:Long,
                      play_times_rank:Long,
                      value_type:String,
                      frequence_type:String
                    ) {

}
