package com.dgmall.sparktest.dgmallTestV2.apps.recommader

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author: Cedaris
  * @Date: 2019/8/8 15:26
  */
object DwdInsertData {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Headline_demo")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql
    sql("show databases").show()
    sql("use headline_test")

    val day2 = "2019-08-08"
    val hour = "14"
    val spe = "$."

    //清洗数据(给每一行数据添加type标签)
    val strsql =
      s"""
        |insert overwrite table tmp_headline_log
        |partition (day='$day2',hour='$hour')
        |select
        |get_json_object(line,'${spe}Type') as log_type,
        |line
        |from ods_headline_log
        |where day='$day2'and hour='$hour'
      """.stripMargin

    //观看视频数据导入
    val strsql2 =
      s"""
        |insert overwrite table dwd_headline_watch
        |PARTITION (day='$day2',hour='$hour')
        |select
        |get_json_object(line, '${spe}distinct_id'),
        |get_json_object(line, '${spe}time'),
        |get_json_object(line, '${spe}Event'),
        |get_json_object(line, '${spe}Type'),
        |get_json_object(line, '${spe}Properties.trace_id'),
        |split(get_json_object(line, '${spe}Properties.trace_id'),'.')[0],
        |split(get_json_object(line, '${spe}Properties.trace_id'),'.')[1],
        |split(get_json_object(line, '${spe}Properties.trace_id'),'.')[2],
        |get_json_object(line, '${spe}Properties.order'),
        |get_json_object(line, '${spe}Properties.user_id'),
        |get_json_object(line, '${spe}Properties.video_id'),
        |get_json_object(line, '${spe}Properties.video_user_id'),
        |get_json_object(line, '${spe}Properties.watch_time_long'),
        |get_json_object(line, '${spe}Properties.is_attention'),
        |get_json_object(line, '${spe}Properties.is_like'),
        |get_json_object(line, '${spe}Properties.is_comment'),
        |get_json_object(line, '${spe}Properties.is_share_weixin'),
        |get_json_object(line, '${spe}Properties.is_share_friendster'),
        |get_json_object(line, '${spe}Properties.is_share_qq'),
        |get_json_object(line, '${spe}Properties.is_save'),
        |get_json_object(line, '${spe}Properties.is_get_red_packets'),
        |get_json_object(line, '${spe}Properties.red_packets_sum'),
        |get_json_object(line, '${spe}Properties.is_copy_site'),
        |get_json_object(line, '${spe}Properties.is_report'),
        |get_json_object(line, '${spe}Properties.report_content'),
        |get_json_object(line, '${spe}Properties.is_not_interested'),
        |get_json_object(line, '${spe}Properties.is_go_shop'),
        |get_json_object(line, '${spe}Properties.shop_id'),
        |get_json_object(line, '${spe}Properties.shop_name')
        |from tmp_headline_log
        |where day='$day2'and hour='$hour'
        |and log_type = 'watch_video'
      """.stripMargin

    //曝光日志导入
    val strsql3 =
      s"""
        |insert overwrite table dwd_headline_view
        |PARTITION (day='$day2',hour='$hour')
        |SELECT
        |get_json_object(line, '${spe}distinct_id'),
        |get_json_object(line, '${spe}time'),
        |get_json_object(line, '${spe}Event'),
        |get_json_object(line, '${spe}Type'),
        |split(get_json_object(line, '${spe}Properties.trace_id'),'.')[0],
        |split(get_json_object(line, '${spe}Properties.trace_id'),'.')[1],
        |split(get_json_object(line, '${spe}Properties.trace_id'),'.')[2],
        |get_json_object(line, '${spe}Properties.user_id'),
        |get_json_object(line, '${spe}Properties.video_id'),
        |get_json_object(line, '${spe}Properties.trace_id')
        |from tmp_headline_log
        |where day='$day2' and hour='$hour'
        |and log_type = 'view'
      """.stripMargin

    //点击日志数据导入
    val strsql4 =
      s"""
        |insert overwrite table dwd_headline_click
        |PARTITION (day='$day2',hour='$hour')
        |SELECT
        |get_json_object(line, '${spe}distinct_id'),
        |get_json_object(line, '${spe}time'),
        |get_json_object(line, '${spe}Event'),
        |get_json_object(line, '${spe}Type'),
        |get_json_object(line, '${spe}Properties.order'),
        |split(get_json_object(line, '${spe}Properties.trace_id'),'.')[0],
        |split(get_json_object(line, '${spe}Properties.trace_id'),'.')[1],
        |split(get_json_object(line, '${spe}Properties.trace_id'),'.')[2],
        |get_json_object(line, '${spe}Properties.user_id'),
        |get_json_object(line, '${spe}Properties.video_id'),
        |get_json_object(line, '${spe}Properties.trace_id')
        |from tmp_headline_log
        |where day='$day2' and hour='$hour'
        |and log_type = 'click'
      """.stripMargin

    //搜索日志数据导入
    val strsql5 =
      s"""
        |insert overwrite table dwd_headline_search
        |PARTITION (day='$day2',hour='$hour')
        |SELECT
        |get_json_object(line, '${spe}distinct_id'),
        |get_json_object(line, '${spe}time'),
        |get_json_object(line, '${spe}Event'),
        |get_json_object(line, '${spe}Type'),
        |get_json_object(line, '${spe}Properties.user_id'),
        |get_json_object(line, '${spe}Properties.search_content')
        |from tmp_headline_log
        |where day='$day2' and hour='$hour'
        |and log_type = 'search_click'
      """.stripMargin

    //送礼日志数据导入
    val strsql6 =
      s"""
        |insert overwrite table dwd_headline_gift
        |PARTITION (day='$day2',hour='$hour')
        |SELECT
        |get_json_object(line, '${spe}distinct_id'),
        |get_json_object(line, '${spe}time'),
        |get_json_object(line, '${spe}Event'),
        |get_json_object(line, '${spe}Type'),
        |get_json_object(line, '${spe}Properties.store_name'),
        |split(get_json_object(line, '${spe}Properties.trace_id'),'.')[0],
        |split(get_json_object(line, '${spe}Properties.trace_id'),'.')[1],
        |split(get_json_object(line, '${spe}Properties.trace_id'),'.')[2],
        |get_json_object(line, '${spe}Properties.user_id'),
        |get_json_object(line, '${spe}Properties.video_id'),
        |get_json_object(line, '${spe}Properties.trace_id')
        |from tmp_headline_log
        |where day='$day2' and hour='$hour'
        |and log_type = 'gift'
      """.stripMargin

    //上传视频信息数据导入
    val strsql7 =
      s"""
        |insert overwrite table dwd_headline_video_info
        |select
        |user_id,
        |video_id,
        |upload_time,
        |video_desc,
        |video_tag,
        |video_child_tag,
        |video_long,
        |music_name,
        |music_write,
        |video_topic,
        |video_address
        |from(
        |SELECT
        |get_json_object(line, '${spe}Properties.user_id')         as user_id,
        |get_json_object(line, '${spe}Properties.video_id')        as video_id,
        |get_json_object(line, '${spe}time')                       as upload_time,
        |get_json_object(line, '${spe}Properties.video_desc')      as video_desc,
        |get_json_object(line, '${spe}Properties.video_tag')       as video_tag,
        |get_json_object(line, '${spe}Properties.video_child_tag') as video_child_tag,
        |get_json_object(line, '${spe}Properties.video_long')      as  video_long,
        |get_json_object(line, '${spe}Properties.music_name')      as music_name,
        |get_json_object(line, '${spe}Properties.music_write')     as music_write,
        |get_json_object(line, '${spe}Properties.video_topic')     as video_topic,
        |get_json_object(line, '${spe}Properties.video_address')   as video_address,
        |row_number () over (partition by get_json_object(line,
        |'${spe}Properties.video_id') order by get_json_object(line, '${spe}time'))  as rank1
        |from tmp_headline_log
        |where day='$day2' and hour='$hour'
        |and log_type = 'release') t1
        |where rank1 = 1
      """.stripMargin

    sql(strsql).show()
    sql(strsql2).show()
    sql(strsql3).show()
    sql(strsql4).show()
    sql(strsql5).show()
    sql(strsql6).show()
    sql(strsql7).show()

    spark.close()
  }
}
