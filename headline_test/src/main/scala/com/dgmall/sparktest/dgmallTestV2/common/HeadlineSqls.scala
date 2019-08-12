package com.dgmall.sparktest.dgmallTestV2.common

/**
  * @Author: Cedaris
  * @Date: 2019/8/9 11:32
  */
object HeadlineSqls {

  val spe = "$."

  /**
    * 原始数据导入语句
    * @param month
    * @param day
    * @param hour
    * @return
    */
  def load_ODS_LOG_SQL(month:String,day:String,hour:String):String={
    val newMonth = month.replace("-","")
    val newDay = day.replace("-","")
    val data_path=s"hdfs://psy831:9000/logData/headline/log/${newMonth}/${newDay}/${newDay}_${hour}"
    val Database = "headline_test"

    val ODS_LOG_SQL=
      s"""
        |load data inpath '$data_path'
        |into table ${Database}.ods_headline_log
        | partition(day='$day',hour='$hour')
      """.stripMargin
    return ODS_LOG_SQL
  }

  /**
    * 清洗数据
    * @param day
    * @param hour
    * @return
    */
  def load_ETL_TEMP_SQL(day:String,hour:String):String={
    val ETL_SQL =
      s"""
         |insert overwrite table tmp_headline_log
         |partition (day='$day',hour='$hour')
         |select
         |get_json_object(line,'${spe}Type') as log_type,
         |line
         |from ods_headline_log
         |where day='$day'and hour='$hour'
      """.stripMargin
    return ETL_SQL
  }

  /**
    * 导入每小时观看视频日志
    * @param day
    * @param hour
    * @return
    */
  def load_DWD_WATCH_LOG(day:String,hour:String):String={
    val DWD_WATCH_LOG = s"""
         |insert overwrite table dwd_headline_watch
         |PARTITION (day='$day',hour='$hour')
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
         |where day='$day'and hour='$hour'
         |and log_type = 'watch_video'
      """.stripMargin
    return DWD_WATCH_LOG
  }

  /**
    * 导入每小时曝光日志
    * @param day
    * @param hour
    * @return
    */
  def load_DWD_VIEW_LOG(day:String,hour:String):String={
    val DWD_VIEW_LOG =
      s"""
         |insert overwrite table dwd_headline_view
         |PARTITION (day='$day',hour='$hour')
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
         |where day='$day' and hour='$hour'
         |and log_type = 'view'
      """.stripMargin
    
    return DWD_VIEW_LOG
  }

  /**
    * 点击日志数据导入
    * @param day
    * @param hour
    * @return
    */
  def load_DWD_CLICK_LOG(day:String,hour:String):String={
    
    val DWD_CLICK_LOG =
      s"""
         |insert overwrite table dwd_headline_click
         |PARTITION (day='$day',hour='$hour')
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
         |where day='$day' and hour='$hour'
         |and log_type = 'click'
      """.stripMargin
    
    return DWD_CLICK_LOG
  }

  /**
    * 搜索日志数据导入
    * @param day
    * @param hour
    * @return
    */
  def load_DWD_SEARCH_LOG(day:String,hour:String):String={
    val DWD_SEARCH_LOG =
      s"""
         |insert overwrite table dwd_headline_search
         |PARTITION (day='$day',hour='$hour')
         |SELECT
         |get_json_object(line, '${spe}distinct_id'),
         |get_json_object(line, '${spe}time'),
         |get_json_object(line, '${spe}Event'),
         |get_json_object(line, '${spe}Type'),
         |get_json_object(line, '${spe}Properties.user_id'),
         |get_json_object(line, '${spe}Properties.search_content')
         |from tmp_headline_log
         |where day='$day' and hour='$hour'
         |and log_type = 'search_click'
      """.stripMargin
    return DWD_SEARCH_LOG
  }

  /**
    * 送礼日志数据导入
    * @param day
    * @param hour
    * @return
    */
  def load_DWD_GIFT_LOG(day:String,hour:String):String={
    val DWD_GIFT_LOG =
      s"""
         |insert overwrite table dwd_headline_gift
         |PARTITION (day='$day',hour='$hour')
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
         |where day='$day' and hour='$hour'
         |and log_type = 'gift'
      """.stripMargin
    return DWD_GIFT_LOG
  }

  /**
    * 上传视频信息数据导入
    * @param day
    * @param hour
    * @return
    */
  def load_UPLOAD_VIDEO_LOG(day:String,hour:String):String={
    val UPLOAD_VIDEO_LOG =
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
         |where day='$day' and hour='$hour'
         |and log_type = 'release') t1
         |where rank1 = 1
      """.stripMargin
    
    return UPLOAD_VIDEO_LOG
  }

  /**
    * 每日视频指标汇总表
    * @param month
    * @param day
    * @return
    */
  def load_VIDEO_SUMMARY_DAY_LOG(month:String,day:String):String={
    val VIDEO_SUMMARY_DAY_LOG =
      s"""
         |INSERT OVERWRITE TABLE dws_video_summary_d
         |PARTITION (month = '${month}' ,day='${day}')
         |SELECT
         |t1.video_id,
         |t1.view_times,
         |t1.view_uv_count,
         |NVL(t2.click_times,0),
         |NVL(t2.click_uv_count,0),
         |NVL(t3.play_times,0),
         |NVL(t3.play_uv_count,0),
         |NVL(t3.play_time_count,0),
         |NVL(t3.video_play_percent,0),
         |NVL(t3.attention_times,0),
         |NVL(t3.like_times,0),
         |NVL(t3.comment_times,0),
         |NVL(t3.share_weixin_times,0),
         |NVL(t3.share_friendster_times,0),
         |NVL(t3.share_qq_times,0),
         |(NVL(t3.share_weixin_times,0) + NVL(t3.share_friendster_times,0)+NVL(t3.share_qq_times,0)) as share_times,
         |NVL(t3.save_times,0),
         |NVL(t3.get_red_packets_times,0),
         |NVL(t3.red_packets_sum_count,0),
         |NVL(t3.copy_site_times,0),
         |NVL(t3.report_times,0),
         |NVL(t3.not_interested_times,0),
         |NVL(t3.go_shop_times,0)
         |FROM
         |(
         |SELECT
         |video_id,
         |count(1) as view_times,
         |count(DISTINCT user_id) as view_uv_count
         |FROM dwd_headline_view
         |WHERE (day = '${day}')
         |GROUP BY video_id ) t1
         |LEFT JOIN (
         |    SELECT
         |    video_id,
         |    count(1) AS click_times,
         |    count(DISTINCT user_id) AS click_uv_count
         |    FROM dwd_headline_click
         |    WHERE (day = '${day}')
         |    GROUP BY video_id
         |    ) t2
         |ON t1.video_id = t2.video_id
         |LEFT JOIN (
         |    SELECT
         |    a.video_id  as video_id,
         |    count(1) as play_times,
         |    count(DISTINCT a.user_id) as play_uv_count,
         |    sum(watch_time_long) as play_time_count,
         |    round(sum(a.watch_time_long)/ sum(if(b.video_long is null ,1,b.video_long)),5) as video_play_percent,
         |    sum(is_attention) as attention_times,
         |    sum(is_like) as like_times,
         |    sum(is_comment) as comment_times,
         |    sum(is_share_weixin) as share_weixin_times,
         |    sum(is_share_friendster) as share_friendster_times,
         |    sum(is_share_qq) as share_qq_times,
         |    sum(is_save) as save_times,
         |    sum(is_get_red_packets) as get_red_packets_times,
         |    sum(red_packets_sum) as red_packets_sum_count,
         |    sum(is_copy_site) as copy_site_times,
         |    sum(is_report) as report_times,
         |    sum(is_not_interested) as not_interested_times,
         |    sum(is_go_shop) as go_shop_times
         |    FROM
         |    (SELECT * FROM
         |    dwd_headline_watch
         |    WHERE day = '${day}') a
         |    LEFT JOIN dwd_headline_video_info b
         |    ON a.video_id = b.video_id
         |    GROUP BY a.video_id
         |    ) t3
         |    ON t1.video_id = t3.video_id
      """.stripMargin
    return VIDEO_SUMMARY_DAY_LOG
  }

  /**
    * 每日用户指标汇总表
    * @param month
    * @param day
    * @return
    */
  def load_USER_SUMMARY_DAY_LOG(month:String,day:String):String={
    val USER_SUMMARY_DAY_LOG =
      s"""
        |INSERT OVERWRITE TABLE dws_user_action_summary_d
        |PARTITION (month = '${month}' , day = '${day}')
        |SELECT
        |t1.user_id,
        |t1.view_times,
        |t1.view_unique_count,
        |NVL(t2.click_times,0),
        |NVL(t2.click_unique_count,0),
        |NVL(t3.play_times,0),
        |NVL(t3.video_play_count,0),
        |NVL(t3.play_time_count,0),
        |NVL(t3.attention_times,0),
        |NVL(t3.like_times,0),
        |NVL(t3.comment_times,0),
        |NVL(t3.share_weixin_times,0),
        |NVL(t3.share_friendster_times,0),
        |NVL(t3.share_qq_times,0),
        |(NVL(t3.share_weixin_times,0) + NVL(t3.share_friendster_times,0) + NVL(t3.share_qq_times,0)) as share_times,
        |NVL(t3.save_times,0),
        |NVL(t3.get_red_packets_times,0),
        |NVL(t3.red_packets_sum_count,0),
        |NVL(t3.copy_site_times,0),
        |NVL(t3.report_times,0),
        |NVL(t3.not_interested_times,0),
        |NVL(t3.go_shop_times,0)
        |FROM
        |(
        |    select
        |        user_id,
        |        count(1) as view_times,
        |        count(DISTINCT video_id) AS view_unique_count
        |    from dwd_headline_view
        |    where (day = '${day}')
        |    group by user_id)
        |    t1
        |LEFT JOIN (
        |    select
        |        user_id,
        |        count(1) as click_times,
        |        count(DISTINCT video_id) AS click_unique_count
        |    from dwd_headline_click
        |    where (day = '${day}')
        |    group by user_id)
        |    t2
        |on t1.user_id = t2.user_id
        |LEFT JOIN (
        |    select
        |        user_id,
        |        count(video_id) as play_times,
        |        count(DISTINCT video_id) as video_play_count,
        |        sum(watch_time_long) as play_time_count,
        |        sum(is_attention) as attention_times,
        |        sum(is_like) as like_times,
        |        sum(is_comment) as comment_times,
        |        sum(is_share_weixin) as share_weixin_times,
        |        sum(is_share_friendster) as share_friendster_times,
        |        sum(is_share_qq) as share_qq_times,
        |        sum(is_save) as save_times,
        |        sum(is_get_red_packets) as get_red_packets_times,
        |        sum(red_packets_sum) as red_packets_sum_count,
        |        sum(is_copy_site) as copy_site_times,
        |        sum(is_report) as report_times,
        |        sum(is_not_interested) as not_interested_times,
        |        sum(is_go_shop) as go_shop_times
        |    from  dwd_headline_watch
        |    where day = '${day}'
        |    group by user_id) t3
        |on t1.user_id = t3.user_id
      """.stripMargin

    return USER_SUMMARY_DAY_LOG
  }

  def load_APP_VIDEO_SUMMARY(month:String,day:String):String={
    val APP_VIDEO_SUMMAY =
      s"""
        |with
        |T1day as
        |(
        |    SELECT
        |    video_id,
        |    view_times as view_times0,
        |    click_times as click_times0,
        |    view_uv_count as view_uv_count0,
        |    click_uv_count as click_uv_count0,
        |    play_time_count as play_time_count0,
        |    play_times as play_times0
        |    FROM
        |    dws_video_summary_d
        |    where day = '${day}'
        |    ),
        |T1week as
        |(
        |    SELECT
        |    video_id,
        |    sum(view_times) as view_times1,
        |    sum(click_times) as click_times1,
        |    sum(view_uv_count) as view_uv_count1,
        |    sum(click_uv_count) as click_uv_count1,
        |    SUM(play_time_count) as play_time_count1,
        |    SUM(play_times) as play_times1
        |    FROM
        |    dws_video_summary_d
        |    where day  BETWEEN date_sub('${day}',7) AND date_sub('${day}',1)
        |    group by video_id
        |    ),
        |T2week as
        |(
        |    SELECT
        |    video_id,
        |    sum(view_times) as view_times2,
        |    sum(click_times) as click_times2,
        |    sum(view_uv_count) as view_uv_count2,
        |    sum(click_uv_count) as click_uv_count2,
        |    SUM(play_time_count) as play_time_count2,
        |    SUM(play_times) as play_times2
        |    FROM
        |    dws_video_summary_d
        |    where day  BETWEEN date_sub('${day}',14) AND date_sub('${day}',1)
        |    group by video_id
        |    ),
        |T1month as
        |(
        |    SELECT
        |    video_id,
        |    sum(view_times) as view_times3,
        |    sum(click_times) as click_times3,
        |    sum(view_uv_count) as view_uv_count3,
        |    sum(click_uv_count) as click_uv_count3,
        |    SUM(play_time_count) as play_time_count3,
        |    SUM(play_times) as play_times3
        |    FROM
        |    dws_video_summary_d
        |    where day  BETWEEN date_sub('${day}',30) AND date_sub('${day}',1)
        |    group by video_id
        |    )
        |
        |NSERT OVERWRITE TABLE app_video_summary
        |PARTITION (month = '${month}' , day = '${day}')
        |select
        |    video_id,
        |    NVL(round(sum(view_times0)/sum(click_times0), 3),0) as ctr_1day,
        |    NVL(round(sum(view_uv_count0)/sum(click_uv_count0), 3),0) as uv_ctr_1day,
        |    NVL(sum(play_time_count0),0) as play_long_1day,
        |    NVL(sum(play_times0),0) as play_times_1day,
        |    NVL(round(sum(view_times1)/sum(click_times1), 3),0) as ctr_1week,
        |    NVL(round(sum(view_uv_count1)/sum(click_uv_count1), 3),0) as uv_ctr_1week,
        |    NVL(SUM(play_time_count1),0) as play_long_1week,
        |    NVL(SUM(play_times1),0) as play_times_1week,
        |    NVL(round(sum(view_times2)/sum(click_times2), 3),0) ctr_2week,
        |    NVL(round(sum(view_uv_count2)/sum(click_uv_count2), 3),0) as uv_ctr_2week,
        |    NVL(SUM(play_time_count2),0) as play_long_2week,
        |    NVL(SUM(play_times2),0) as play_times_2week,
        |    NVL(round(sum(view_times3)/sum(click_times3), 3),0) as ctr_1month,
        |    NVL(round(sum(view_uv_count3)/sum(click_uv_count3), 3),0) as uv_ctr_1month,
        |    NVL(SUM(play_time_count3),0) as play_long_1month,
        |    NVL(SUM(play_times3),0) as play_times_1month
        |FROM
        |(
        |select
        |    video_id,
        |    view_times0,
        |    click_times0,
        |    view_uv_count0,
        |    click_uv_count0,
        |    play_time_count0,
        |    play_times0,
        |
        |    0 view_times1,
        |    0 click_times1,
        |    0 view_uv_count1,
        |    0 click_uv_count1,
        |    0 play_time_count1,
        |    0 play_times1,
        |
        |    0 view_times2,
        |    0 click_times2,
        |    0 view_uv_count2,
        |    0 click_uv_count2,
        |    0 play_time_count2,
        |    0 play_times2,
        |
        |    0 view_times3,
        |    0 click_times3,
        |    0 view_uv_count3,
        |    0 click_uv_count3,
        |    0 play_time_count3,
        |    0 play_times3
        |from T1day
        |UNION ALL
        |
        |select
        |    video_id,
        |    0 view_times0,
        |    0 click_times0,
        |    0 view_uv_count0,
        |    0 click_uv_count0,
        |    0 play_time_count0,
        |    0 play_times0,
        |
        |    view_times1,
        |    click_times1,
        |    view_uv_count1,
        |    click_uv_count1,
        |    play_time_count1,
        |    play_times1,
        |
        |    0 view_times2,
        |    0 click_times2,
        |    0 view_uv_count2,
        |    0 click_uv_count2,
        |    0 play_time_count2,
        |    0 play_times2,
        |
        |    0 view_times3,
        |    0 click_times3,
        |    0 view_uv_count3,
        |    0 click_uv_count3,
        |    0 play_time_count3,
        |    0 play_times3
        |from T1week
        |UNION ALL
        |SELECT
        |    video_id,
        |    0 view_times0,
        |    0 click_times0,
        |    0 view_uv_count0,
        |    0 click_uv_count0,
        |    0 play_time_count0,
        |    0 play_times0,
        |
        |    0 view_times1,
        |    0 click_times1,
        |    0 view_uv_count1,
        |    0 click_uv_count1,
        |    0 play_time_count1,
        |    0 play_times1,
        |
        |    view_times2,
        |    click_times2,
        |    view_uv_count2,
        |    click_uv_count2,
        |    play_time_count2,
        |    play_times2,
        |
        |    0 view_times3,
        |    0 click_times3,
        |    0 view_uv_count3,
        |    0 click_uv_count3,
        |    0 play_time_count3,
        |    0 play_times3
        |from T2week
        |UNION ALL
        |SELECT
        |    video_id,
        |    0 view_times0,
        |    0 click_times0,
        |    0 view_uv_count0,
        |    0 click_uv_count0,
        |    0 play_time_count0,
        |    0 play_times0,
        |
        |    0 view_times1,
        |    0 click_times1,
        |    0 view_uv_count1,
        |    0 click_uv_count1,
        |    0 play_time_count1,
        |    0 play_times1,
        |
        |    0 view_times2,
        |    0 click_times2,
        |    0 view_uv_count2,
        |    0 click_uv_count2,
        |    0 play_time_count2,
        |    0 play_times2,
        |
        |    view_times3,
        |    click_times3,
        |    view_uv_count3,
        |    click_uv_count3,
        |    play_time_count3,
        |    play_times3
        |FROM T1month
        |    ) temp_video_summary
        |group by video_id
      """.stripMargin

    return APP_VIDEO_SUMMAY
  }

}
