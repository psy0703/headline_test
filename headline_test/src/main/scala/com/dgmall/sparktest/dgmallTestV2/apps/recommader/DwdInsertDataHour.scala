package com.dgmall.sparktest.dgmallTestV2.apps.recommader
import org.apache.spark.sql.SparkSession

/**
  * 家乡头条数仓dwd层导数据（每小时）
  * @Author: Cedaris
  * @Date: 2019/8/8 15:26
  */
object DwdInsertDataHour {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("DwdInsertDataHour")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    System.setProperty("HADOOP_USER_NAME", "psy831")
    //允许笛卡尔积
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    import spark.sql
    sql("show databases").show()
    sql("use headline_test")

    val month = "2019-08"
    val day = "2019-08-15"
    var hour = "10"
    val spe = "$."

    import com.dgmall.sparktest.dgmallTestV2.common.HeadlineSqls._

    for(i <- 11 to 18){
      hour = i + ""
      println(month + "\t" + day + "\t" + hour)

      //原始数据导入
      sql(load_ODS_LOG_SQL(month,day,hour)).show()
      //数据清洗
      sql(load_ETL_TEMP_SQL(day,hour)).show()
      //曝光日志
      sql(load_DWD_VIEW_LOG(day,hour)).show()
      //点击日志
      sql(load_DWD_CLICK_LOG(day,hour)).show()
      //观看日志
      sql(load_DWD_WATCH_LOG(day,hour)).show()
      //搜索日志
      sql(load_DWD_SEARCH_LOG(day,hour)).show()
      //送礼日志
      sql(load_DWD_GIFT_LOG(day,hour)).show()
      //上传视频信息
      sql(load_UPLOAD_VIDEO_LOG(day,hour)).show()
    }


    println("导入成功~~")
    spark.close()
  }

}
