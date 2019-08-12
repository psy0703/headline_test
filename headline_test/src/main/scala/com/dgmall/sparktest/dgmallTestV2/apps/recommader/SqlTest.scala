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
      """
        |SELECT
        |t3.user_id,
        |t3.video_tag,
        |round(t3.sum_watchtime/t4.sum_watchtime2,3) as weights_cate1,
        |t3.rank1
        |FROM(
        |    SELECT
        |    t1.user_id,
        |    t2.video_tag,
        |    count(t1.video_id) as video_count,
        |    sum(t1.watch_time_long) as sum_watchtime,
        |    row_number() over(partition by t1.user_id order by sum
        |    (t1.watch_time_long)) as rank1
        |    FROM dwd_headline_watch t1
        |    left join dwd_headline_video_info t2
        |    on t1.video_id = t2.video_id
        |    where t2.video_id is not null
        |    group by t1.user_id,t2.video_tag
        |    order by t1.user_id,video_count
        |    --limit 5
        |    ) t3
        |join(
        |    SELECT
        |    t1.user_id,
        |    sum(t1.watch_time_long) as sum_watchtime2
        |    FROM dwd_headline_watch t1
        |    left join dwd_headline_video_info t2
        |    on t1.video_id = t2.video_id
        |    where t2.video_id is not null
        |    group by t1.user_id
        |    order by t1.user_id
        |    ) t4
        |on t3.user_id = t4.user_id
      """.stripMargin

    sql(mySql).show()

    spark.close()
  }
}
