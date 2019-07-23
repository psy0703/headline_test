package com.dgmall.sparktest.dgmallTest.app.structced

import com.alibaba.fastjson.JSON
import com.dgmall.sparktest.dgmallTest.appbean.AppWatch
import com.dgmall.sparktest.dgmallTest.bean.{CommenRecord, WatchTable}
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @Author: Cedaris
  * @Date: 2019/7/22 16:53
  */
object StructedMain {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
      .set("spark.debug.maxToStringFields", "100")
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

    val df: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "psy831:9092,psy832:9092,psy833:9092")
      .option("subscribe", "dgmall_log")
      .load()

    //将kafka流数据转换为K-V为String的DataSet
    val ds: Dataset[(String, String)] = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    //转化为日志格式的样式
    val RecordDS: Dataset[CommenRecord] = ds.map(x => {
      val records: String = x._2
      val json: CommenRecord = JSON.parseObject(records, classOf[CommenRecord])
      json
    })
    //过滤观看记录
    val watchRecord: Dataset[WatchTable] = RecordDS
      .filter(x => if (x.Event.contains("play")) true else false)
      .map(
        x => {
          val prop: String = x.Properties
          val watch: AppWatch = JSON.parseObject(prop, classOf[AppWatch])
          //转换为观看表结构的类
          val wacthTable = WatchTable(x.distinct_id, x.Time, x.Event, x.Type,
            watch.getTrace_id, watch.getAlg_match, watch.getAlg_rank, watch.getRule,
            watch.getBhv_amt, watch.getUser_id, watch.getVideo_id,
            watch.getVideo_user_id, watch.getVideo_desc, watch.getVideo_tag,
            watch.getWatch_time_long, watch.getVideo_long, watch.getMusic_name,
            watch.getMusic_write, watch.getVideo_topic, watch.getVideo_address,
            watch.getIs_attention, watch.getIs_like, watch.getIs_comment,
            watch.getIs_share_weixin, watch.getIs_share_friendster,
            watch.getIs_share_qq, watch.getIs_save, watch.getIs_get_red_packets,
            watch.getRed_packets_sum, watch.getIs_copy_site, watch.getIs_report,
            watch.getReport_content, watch.getIs_not_interested,
            watch.getIs_go_shop, watch.getShop_id, watch.getShop_name)

          wacthTable
        }
      )
    watchRecord.createOrReplaceTempView("watch_log")

    val query: StreamingQuery = sql("select count(*) from watch_log")
      //      .dropDuplicates("user_id")
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

    spark.close()
  }

}
