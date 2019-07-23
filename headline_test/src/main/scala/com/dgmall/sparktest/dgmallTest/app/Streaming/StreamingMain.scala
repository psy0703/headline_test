package com.dgmall.sparktest.dgmallTest.app.Streaming

import java.lang

import com.alibaba.fastjson.JSON
import com.dgmall.sparktest.dgmallTest.appbean.{AppClick, AppView, AppWatch}
import com.dgmall.sparktest.dgmallTest.bean.{ClickTable, CommenRecord, ViewTable, WatchTable}
import com.dgmall.sparktest.dgmallTest.common.ParseObject
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Minutes, Seconds, State, StateSpec, StreamingContext}

/**
  * @Author: Cedaris
  * @Date: 2019/7/17 16:46
  */
object StreamingMain {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .set("spark.streaming.kafka.consumer.cache.enabled","false")
      .set("spark.debug.maxToStringFields","100")
//      .set("hive.exec.dynamic.parition", "true")
//      .set("hive.exec.dynamic.parition.mode", "nonstrict")
//      .set("spark.sql.warehouse.dir", "hdfs://psy831:9000/user/hive/warehouse")
//      .set("javax.jdo.option.ConnectionURL", "jdbc:mysql://psy831:3306/hive?characterEncoding=UTF-8")
//      .set("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
//      .set("javax.jdo.option.ConnectionUserName", "root")
//      .set("javax.jdo.option.ConnectionPassword", "root")
      .set("spark.driver.allowMultipleContexts","true")
      .setAppName("address")
      .setMaster("local[*]")

    /*val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
*/
    /*import spark.implicits._
    import spark.sql*/

    val ssc = new StreamingContext(sparkConf,Seconds(60))
//    ssc.checkpoint("/checkpoint")

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
//    val stateSpec: StateSpec[String, Long, Long, (String, Long)] = StateSpec.function(mapFunction)

    //读取kafka的数据并转化为日志格式
    val recordDs: DStream[CommenRecord] = stream.map(x => {
      val records: String = x.value()
      val json: CommenRecord = JSON.parseObject(records, classOf[CommenRecord])
      json
    })
    //提取处理观看日志
    /*val watchDS: DStream[WatchTable] = recordDs
      .filter(x => {if (x.Event.contains("play")) true else false}) //过滤观看日志
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
          .getIs_go_shop, watch.getShop_id, watch.getShop_name)})

    watchDS.foreachRDD(x => {
      val sQLContext: SQLContext = SQLContextSingleton.getInstance(x.sparkContext)
      import sQLContext.implicits._
        x.map(y => y).toDF().registerTempTable("watch")
      //统计一分钟的PV,UV
      val logCount: DataFrame = sQLContext.sql(
        "select " +
          "date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss') as time," +
          "count(1) as pv," +
          "count(distinct user_id) as uv " +
          "from watch")
      //打印结果
      logCount.show()
    })*/

    val sc: SQLContext = SQLContextSingleton.getInstance(ssc.sparkContext)
    import sc.implicits._
    import sc.sql

    //提取处理曝光和点击日志
    val viewDS: DStream[ViewTable] = ParseObject.parseView(recordDs)
    val clickDS: DStream[ClickTable] = ParseObject.parseClick(recordDs)

    viewDS.foreachRDD(x => {
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

    clickDS.foreachRDD(x => {
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

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(false, true)    //优雅地结束
  }

}
object SQLContextSingleton {
  @transient  private var instance: SQLContext = _
  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
