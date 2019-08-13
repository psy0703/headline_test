package com.dgmall.sparktest.dgmallTestV2.apps.recommader

import org.apache.spark.sql.SparkSession

/**
  * @Author: Cedaris
  * @Date: 2019/8/12 9:14
  */
object SqlTest {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("SqlTest")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    //运行笛卡尔积
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    System.setProperty("HADOOP_USER_NAME", "psy831")

    import spark.implicits._
    import spark.sql
//    sql("show databases").show()
    sql("use headline_test")

    val month = "2019-08"
    val day = "2019-08-07"
    val spe = "$."

    import com.dgmall.sparktest.dgmallTestV2.common.HeadlineSqls._

    val mySql =
      s"""
        |with
        |temp1 as (
        |    SELECT
        |    t2.user_id,
        |    t2.time,
        |    t2.watch_last_time,
        |    t2.timeSinceLastWatch,
        |    round(sqrt(t2.timeSinceLastWatch),2) as timeSinceLastWatchSqrt,
        |    pow(t2.timeSinceLastWatch,2) as timeSinceLastWatchSquare
        |FROM(
        |    SELECT
        |        t1.user_id,
        |        t1.time,
        |        t1.watch_last_time,
        |        if(t1.watch_last_time is not null,(unix_timestamp(t1.time) - unix_timestamp(t1.watch_last_time)),0) as timeSinceLastWatch
        |    FROM(
        |        SELECT
        |            user_id,
        |            time,
        |            LAG(time,1) OVER(partition by  user_id order by `time`) as watch_last_time,
        |            row_number() OVER(partition by  user_id order by `time` desc) as rank1
        |        FROM dwd_headline_watch
        |        where day <= '2019-08-12') t1
        |        where rank1 = 1
        |)t2
        |),
        |
 |temp2 as(
        |    SELECT
        |    t3.user_id,
        |    collect_list(t3.video_id) as behaviorVids,
        |    collect_list(t3.upload_user) as behaviorAids,
        |    collect_list(t3.video_tag) as behaviorCids,
        |    collect_list(t3.video_child_tag) as behaviorC1ids
        |    FROM(
        |    SELECT
        |    t1.user_id,
        |    t1.video_id,
        |    row_number() over(partition by t1.user_id order by t1.time desc) as rank1,
        |    NVL(t2.video_tag,0) as video_tag,
        |    NVL(t2.video_child_tag,0) as video_child_tag,
        |    NVL(t2.user_id,'0') as upload_user
        |    from dwd_headline_watch t1
        |    left join dwd_headline_video_info t2
        |    on t1.video_id = t2.video_id
        |    ) t3
        |    WHERE t3.rank1 <= 10
        |    group by t3.user_id
        |    ),
        |
 |temp3 as (
        |    SELECT
        |    t1.user_id,
        |    collect_list(t1.search_content) as behaviorTokens
        |    FROM(
        |    SELECT
        |    user_id,
        |    time,
        |    search_content,
        |    row_number() over(partition by user_id order by time desc) as rank1
        |    FROM dwd_headline_search
        |    where day <= '2019-08-13'
        |    ) t1
        |    where t1.rank1 <= 10
        |    group by t1.user_id
        |    ),
        |
 |temp4 as (
        |  SELECT
        |    tmp1.user_id,
        |    cate1_prefer,
        |    weights_cate1_prefer,
        |    cate2_prefer,
        |    weights_cate2_prefer
        |FROM(
        |    select
        |        t5.user_id,
        |        collect_list(t5.video_tag) as cate1_prefer,
        |        collect_list(t5.weights_cate1) as weights_cate1_prefer
        |    FROM(
        |    SELECT
        |        t3.user_id,
        |        t3.video_tag,
        |        round(t3.sum_watchtime/t4.sum_watchtime2,3) as weights_cate1
        |    FROM(
        |        SELECT
        |        t1.user_id,
        |        t2.video_tag,
        |        count(t1.video_id) as video_count,
        |        sum(t1.watch_time_long) as sum_watchtime,
        |        row_number() over(partition by t1.user_id order by sum(t1.watch_time_long)) as rank1
        |        FROM dwd_headline_watch t1
        |        left join dwd_headline_video_info t2
        |        on t1.video_id = t2.video_id
        |        where day <= '2019-08-13'
        |        AND t2.video_id is not null
        |        group by t1.user_id,t2.video_tag
        |        order by t1.user_id,video_count
        |        ) t3
        |    join(
        |        SELECT
        |        t1.user_id,
        |        sum(t1.watch_time_long) as sum_watchtime2
        |        FROM dwd_headline_watch t1
        |        left join dwd_headline_video_info t2
        |        on t1.video_id = t2.video_id
        |        where day <= '2019-08-13'
        |        AND t2.video_id is not null
        |        group by t1.user_id
        |        order by t1.user_id
        |        ) t4
        |    on t3.user_id = t4.user_id
        |    where t3.rank1 <= 5
        |    )t5
        |    GROUP BY t5.user_id
        |) tmp1
        |inner join
        |    (SELECT
        |        t5.user_id,
        |        collect_set(t5.video_child_tag) as cate2_prefer,
        |        collect_set(t5.weights_cate2) as weights_cate2_prefer
        |    FROM(
        |        SELECT
        |            t3.user_id,
        |            t3.video_child_tag,
        |            round(t3.sum_watchtime/t4.sum_watchtime2,3) as weights_cate2
        |        FROM(
        |            SELECT
        |                t1.user_id,
        |                t2.video_child_tag,
        |                count(t1.video_id) as video_count,
        |                sum(t1.watch_time_long) as sum_watchtime,
        |                row_number() over(partition by t1.user_id order by sum(t1.watch_time_long)) as rank1
        |            FROM dwd_headline_watch t1
        |            left join dwd_headline_video_info t2
        |            on t1.video_id = t2.video_id
        |            where day <= '2019-08-13'
        |            AND t2.video_id is not null
        |            group by t1.user_id,t2.video_child_tag
        |            order by t1.user_id,video_count
        |            ) t3
        |    join(
        |            SELECT
        |            t1.user_id,
        |            sum(t1.watch_time_long) as sum_watchtime2
        |            FROM dwd_headline_watch t1
        |            left join dwd_headline_video_info t2
        |            on t1.video_id = t2.video_id
        |            where day <= '2019-08-13'
        |            AND t2.video_id is not null
        |            group by t1.user_id
        |            order by t1.user_id
        |        ) t4
        |    on t3.user_id = t4.user_id
        |    where t3.rank1 <= 5
        |    )t5
        |    GROUP BY t5.user_id
        |    ) tmp2
        |on tmp1.user_id = tmp2.user_id
        |)
        |
 |INSERT OVERWRITE TABLE app_user_actions_summary
        |select
        |temp1.user_id,
        |temp1.time,
        |temp1.watch_last_time,
        |temp1.timeSinceLastWatch,
        |temp1.timeSinceLastWatchSqrt,
        |temp1.timeSinceLastWatchSquare,
        |temp2.behaviorVids,
        |temp2.behaviorAids,
        |temp2.behaviorCids,
        |temp2.behaviorC1ids,
        |temp3.behaviorTokens,
        |temp4.cate1_prefer,
        |temp4.weights_cate1_prefer,
        |temp4.cate2_prefer,
        |temp4.weights_cate2_prefer
        |from
        |temp1
        |join temp2
        |on temp1.user_id = temp2.user_id
        |join temp3
        |on temp1.user_id = temp3.user_id
        |join temp4
        |on temp1.user_id = temp4.user_id
      """.stripMargin

    sql(mySql).show()
    println("操作成功！！！")

    spark.close()
  }
}
