package caseclass

/**
  * 对应Hive 表 app_video_summary
  * @Author: Cedaris
  * @Date: 2019/8/15 15:00
  */
case class VideoIndexSummay (
                            video_id:String,
                            ctr_1day:Double,
                            uv_ctr_1day:Double,
                            play_long_1day:Double,
                            play_times_1day:Double,
                            ctr_1week:Double,
                            uv_ctr_1week:Double,
                            play_long_1week:Double,
                            play_times_1week:Double,
                            ctr_2week:Double,
                            uv_ctr_2week:Double,
                            play_long_2week:Double,
                            play_times_2week:Double,
                            ctr_1month:Double,
                            uv_ctr_1month:Double,
                            play_long_1month:Double,
                            play_times_1month:Double
                            ){

}
