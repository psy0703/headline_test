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
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    System.setProperty("HADOOP_USER_NAME", "psy831")

    import spark.implicits._
    import spark.sql
    sql("show databases").show()
    sql("use headline_test")

    val month = "2019-08"
    val day = "2019-08-07"


    val spe = "$."

    import com.dgmall.sparktest.dgmallTestV2.common.HeadlineSqls._

    //视频信息汇总
    sql(load_VIDEO_SUMMARY_DAY_LOG(month, day)).show()
    //用户行为汇总
    sql(load_USER_SUMMARY_DAY_LOG(month, day)).show()
    //应用层视频信息汇总（天、周、月）
    sql(load_APP_VIDEO_SUMMARY(month, day)).show()

    spark.close()
  }
}
