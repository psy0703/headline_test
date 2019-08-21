package com.dgmall.sparktest.dgmallTestV2.apps.realtime

import java.lang
import java.util.Date

import com.alibaba.fastjson.JSON
import com.dgmall.sparktest.dgmallTestV2.caseclass.CommenRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * @Author: Cedaris
  * @Date: 2019/8/21 15:58
  */
object RealTimeMain {

  private val LOG = LoggerFactory.getLogger("RealTimeMain")
  private val STOP_FLAG = "TEST_STOP_FLAG"

  def main(args: Array[String]): Unit = {

    //spark conf 配置
    val batch = 10
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("RealTimeMain")
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
      .set("spark.debug.maxToStringFields", "100")
      .setIfMissing("spark.master", "local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "20000")

    System.setProperty("HADOOP_USER_NAME", "dev")


    //定义kafka参数
    val bootstrap: String = "dev-node01:9092,dev-node02:9092,dev-node03:9092"
    val topic: Array[String] = Array("dgmall_log")
    val consumergroup: String = "headline_test05"

    //将kafka参数映射为map
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> bootstrap,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> consumergroup,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    //计算当前时间距离次日凌晨的时长(毫秒数)
    def resetTime = {
      val now = new Date()
      val tomorrowMidnight = new Date(now.getYear, now.getMonth, now.getDate + 1)
      tomorrowMidnight.getTime - now.getTime
    }

    //实时流量状态更新函数
    val mapFuction = (name: String, pv: Option[Int], state: State[Int]) => {
      val accuSum = pv.getOrElse(0) + state.getOption().getOrElse(0)
      val output = (name, accuSum)
      state.update(accuSum)
      output
    }
    val stateSpec = StateSpec.function(mapFuction)


    while (true) {
      //Spark streaming 配置
      val ssc = new StreamingContext(sparkConf, Seconds(batch))
      ssc.checkpoint("hdfs://dev-node02:9000/spark/checkpoint/RealTimeMain")

      //通过KafkaUtil创建KafkaDStream
      val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
        ssc,
        PreferConsistent,
        Subscribe[String, String](topic, kafkaParams)
      )


      //读取kafka的数据并转化为日志格式
      val recordDs: DStream[CommenRecord] = kafkaStream.map(x => {
        val records: String = x.value()
        val json: CommenRecord = JSON.parseObject(records, classOf[CommenRecord])
        json
      })

      //曝光日志
      val view: DStream[(String, Int)] = recordDs
        .filter(y => if ("view".equals(y.Type)) true else false)
        .map(record => {
          Tuple2("view", 1)
        })
      //点击日志
      val click: DStream[(String, Int)] = recordDs
        .filter(y => if ("click".equals(y.Type)) true else false)
        .map(record => {
          Tuple2("click", 1)
        })

      //点击日志
      val watch: DStream[(String, Int)] = recordDs
        .filter(y => if ("watch_video".equals(y.Type)) true else false)
        .map(record => {
          Tuple2("watch", 1)
        })


      //生成曝光实时累计状态
      view.mapWithState(stateSpec).stateSnapshots().foreachRDD(x=> {
        x.foreach(y=>{
          val date = new Date()
          val str: String = date+ "\t" + y._1 + "\t" + y._2
          println(str)
        })
      })

      click.mapWithState(stateSpec).stateSnapshots().foreachRDD(x=> {
        x.foreach(y=>{
          val date = new Date()
          val str: String = date+ "\t" + y._1 + "\t" + y._2
          println(str)
        })
      })

      //生成观看实时累计状态
      watch.mapWithState(stateSpec).stateSnapshots().foreachRDD(x=> {
        x.foreach(y=>{
          val date = new Date()
          val str: String = date+ "\t" + y._1 + "\t" + y._2
          println(str)
        })
      })

      ssc.start()
      ssc.awaitTerminationOrTimeout(resetTime)
      ssc.stop(false, true)
    }

  }

  //获取sparkSession 的单例类
  object SparkSessionSingleton {
    @transient private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .enableHiveSupport()
          .config("spark.sql.crossJoin.enabled", "true")
          .getOrCreate()
      }
      instance
    }
  }


  //用户id和视频id 样例类
  case class UserVideo(user_id: String, video_id: String) {}

  case class UserVideoClick(user_id: String, video_id: String, is_click: Int) {}

}
