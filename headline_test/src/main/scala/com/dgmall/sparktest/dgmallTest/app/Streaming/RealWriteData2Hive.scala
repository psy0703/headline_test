package com.dgmall.sparktest.dgmallTest.app.Streaming

import java.lang
import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.dgmall.sparktest.dgmallTest.appbean.{AppClick, AppView, AppWatch}
import com.dgmall.sparktest.dgmallTest.bean.{ClickTable, CommenRecord, ViewTable, WatchTable}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Minutes, Seconds, State, StateSpec, StreamingContext}

/**
  * 读取kafka数据写入hive
  * @Author: Cedaris
  * @Date: 2019/7/24 14:31
  */
object RealWriteData2Hive {

  def main(args: Array[String]): Unit = {

    val batch = 10

    val sparkConf: SparkConf = new SparkConf()
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
      .set("spark.debug.maxToStringFields", "100")
      .set("hive.exec.dynamic.parition", "true")
      .set("hive.exec.dynamic.parition.mode", "nonstrict")
      .set("spark.sql.warehouse.dir", "hdfs://psy831:9000/user/hive/warehouse")
      .set("javax.jdo.option.ConnectionURL", "jdbc:mysql://psy831:3306/hive?characterEncoding=UTF-8")
      .set("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
      .set("javax.jdo.option.ConnectionUserName", "root")
      .set("javax.jdo.option.ConnectionPassword", "root")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.debug.maxToStringFields","1000")
      .setAppName("address")
      .setMaster("local[*]")

    //创建Spark Streaming Context
    val ssc = new StreamingContext(sparkConf, Seconds(batch)) //60秒计算一次
    System.setProperty("HADOOP_USER_NAME", "psy831")
    ssc.checkpoint("hdfs://psy831:9000/checkpoint/test")

    //定义kafka参数
    val bootstrap: String = "psy831:9092,psy832:9092,psy833:9092"
    val topic: Array[String] = Array("dgmall_log")
    val consumergroup: String = "dgmall"
    //将kafka参数映射为map
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> bootstrap,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> consumergroup,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    //通过KafkaUtil创建KafkaDStream
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      PreferConsistent,
      Subscribe[String, String](topic, kafkaParams)
    )

    //读取kafka的数据并转化为日志格式
    val recordDs: DStream[CommenRecord] = stream.map(x => {
      val records: String = x.value()
      val json: CommenRecord = JSON.parseObject(records, classOf[CommenRecord])
      json
    })
    val sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm")
    val partition: String = sdf.format(System.currentTimeMillis())
    recordDs
      .window(Seconds(batch * 3), Seconds(batch * 3))
      .saveAsTextFiles(s"hdfs://psy831:9000/output/${partition}/")

    /*val spark: SparkSession = SparkSessionSingleton.getInstance(ssc.sparkContext)
    import spark.implicits._
    import spark.sql

    //提取处理观看日志
    val watchDS: DStream[WatchTable] = recordDs
      .window(Seconds(batch * 3), Seconds(batch * 3))
      .filter(x => {
        if (x.Event.contains("play")) true else false
      }) //过滤观看日志
      .map(x => {
      val properties: String = x.Properties.toString
      val watch: AppWatch = JSON.parseObject(properties, classOf[AppWatch])
      //转换为观看表结构的类
      WatchTable(x.distinct_id, x.Time, x.Event, x.Type, watch.getTrace_id,
        watch.getAlg_match, watch.getAlg_rank, watch.getRule, watch
          .getBhv_amt, watch.getUser_id, watch.getVideo_id, watch
          .getVideo_user_id, watch.getVideo_desc, watch.getVideo_tag, watch
          .getWatch_time_long, watch.getVideo_long, watch.getMusic_name, watch
          .getMusic_write, watch.getVideo_topic, watch.getVideo_address, watch
          .getIs_attention, watch.getIs_like, watch.getIs_comment, watch
          .getIs_share_weixin, watch.getIs_share_friendster, watch
          .getIs_share_qq, watch.getIs_save, watch.getIs_get_red_packets,
        watch.getRed_packets_sum, watch.getIs_copy_site, watch.getIs_report,
        watch.getReport_content, watch.getIs_not_interested, watch
          .getIs_go_shop, watch.getShop_id, watch.getShop_name)
    })

    watchDS.foreachRDD(x => {
      val df: DataFrame = x.map(y => y).toDF()
      df.createOrReplaceTempView("watch")

      // Create a Hive partitioned table using DataFrame API
      val sdf = new SimpleDateFormat("yyyy-MM-dd-HH")
      val partition: String = sdf.format(System.currentTimeMillis())
      sql(
        s"""
           |select
           |*
           |from watch
          """.stripMargin)
        .write
        .mode(SaveMode.Append)
        .parquet(s"hdfs://psy831:9000/output/${partition}/")

      spark.close()
    })*/


    /*//提取处理曝光和点击日志
    val viewDS: DStream[ViewTable] = ParseObject.parseView(recordDs)
    val clickDS: DStream[ClickTable] = ParseObject.parseClick(recordDs)

    //计算view和click的PV
    val viewPv: DStream[Long] = viewDS
      .countByWindow(Seconds(batch * 6), Seconds(batch * 6))
    val clickPv: DStream[Long] = clickDS
      .countByWindow(Seconds(batch * 6), Seconds(batch * 6))

    //计算 view和click 的UV
    viewDS
      .window(Seconds(batch * 6), Seconds(batch * 6))
      .foreachRDD(x => {
        x.toDF().createOrReplaceTempView("view")
        //统计一分钟的PV,UV
        val logCount: DataFrame = sql(
          "select " +
            "date_format(current_timestamp(),'yyyy-MM-dd HH:mm') as time," +
            "count(1) as pv," +
            "count(distinct user_id) as uv " +
            "from view")
        logCount.createOrReplaceTempView("ViewCount")
        //打印结果
        logCount.show()
      })

    clickDS
      .window(Seconds(batch * 6), Seconds(batch * 6))
      .foreachRDD(x => {
        x.toDF().createOrReplaceTempView("click")
        //统计一分钟的PV,UV
        val logCount: DataFrame = sql(
          """
            |select
            |date_format(current_timestamp(),'yyyy-MM-dd HH:mm') as time,
            |count(1) as Click_Sum
            |from click
          """.stripMargin)
        logCount.createOrReplaceTempView("ClickCount")
        //打印结果
        logCount.show()
      })

    viewPv.map(x => "view_pv : " + x).print()
    clickPv.map(x => "click_pv : " + x).print()*/

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(false, true) //优雅地结束
  }

}

object SparkSessionSingleton {
  @transient private var instance: SparkSession = _

  def getInstance(sparkContext: SparkContext): SparkSession = {
    if (instance == null) {
      val conf: SparkConf = sparkContext.getConf
      val spark: SparkSession = SparkSession.builder()
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()
      instance = spark
    }
    instance
  }
}
