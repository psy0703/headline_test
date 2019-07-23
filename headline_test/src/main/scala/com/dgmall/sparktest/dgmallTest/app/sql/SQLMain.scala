package com.dgmall.sparktest.dgmallTest.app.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Author: Cedaris
  * @Date: 2019/7/18 15:29
  */
object SQLMain {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .set("spark.streaming.kafka.consumer.cache.enabled","false")
      .set("spark.debug.maxToStringFields","100")
      .setAppName("OmallTest")
      .setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .config("hive.exec.dynamic.parition", "true")
      .config("hive.exec.dynamic.parition.mode", "nonstrict")
      .config("spark.sql.warehouse.dir", "hdfs://psy831:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql


  }

}
