package com.dgmall.sparktest.dgmallTestV2.apps.recommader

import org.apache.spark.sql.SparkSession

/**
  * @Author: Cedaris
  * @Date: 2019/8/9 14:59
  */
object DwsInsertDataDay {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("DwsInsertDataDay")
      .master("local[4]")
      .enableHiveSupport()
      .getOrCreate()

    System.setProperty("HADOOP_USER_NAME", "psy831")
    //允许笛卡尔积
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    import spark.implicits._
    import spark.sql
//    sql("show databases").show()
    sql("use headline_test")

    val month = "2019-08"
    val day = "2019-08-14"


    val spe = "$."

    import com.dgmall.sparktest.dgmallTestV2.common.HeadlineSqls._

    //视频信息汇总
    sql(load_VIDEO_SUMMARY_DAY_LOG(month, day)).show()
    //用户行为汇总
    sql(load_USER_SUMMARY_DAY_LOG(month, day)).show()
    //用户等级
    sql(load_APP_USER_LEVEL(day)).show()
    //应用层视频信息汇总（天、周、月）
    sql(load_APP_VIDEO_SUMMARY(month, day)).show()
    //应用层用户最近行为汇总
    sql(load_APP_USER_ACTIONS_SUMMARY(day)).show()

    println("导入成功~~")
    spark.close()
  }
}
