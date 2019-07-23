package com.dgmall.sparktest.dgmallTest.app.Streaming.PV_UV

import java.lang

import com.alibaba.fastjson.JSON
import com.dgmall.sparktest.dgmallTest.bean.{ClickTable, CommenRecord, ViewTable}
import com.dgmall.sparktest.dgmallTest.common.{InternalRedisClient, ParseObject, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, Pipeline}

/**
  * @Author: Cedaris
  * @Date: 2019/7/23 10:05
  */
object pv_uv_Redis {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("pv_uv")
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
      .set("spark.debug.maxToStringFields", "100")
      .setIfMissing("spark.master", "local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "20000")

    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.sparkContext.setCheckpointDir("hdfs://psy831:9000//checkpoint")
    System.setProperty("HADOOP_USER_NAME", "psy831")

    //定义kafka参数
    val bootstrap: String = "psy831:9092,psy832:9092,psy833:9092"
    val topic: Array[String] = Array("dgmall_log")
    val consumergroup: String = "uv_pv"
    val partition: Int = 0 //测试topic只有一个分区
    val start_offset: Long = 0l
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

    //提取处理曝光和点击日志
    val viewDS: DStream[ViewTable] = ParseObject.parseView(recordDs)
    val clickDS: DStream[ClickTable] = ParseObject.parseClick(recordDs)

    //曝光的PV UV
    viewDS.foreachRDD(x=>{
      val jedis: Jedis = RedisUtil.getJedisClient
      val p1: Pipeline = jedis.pipelined()
//      //开启事务
//      p1.multi()
      val viewTables: Array[ViewTable] = x.collect()
      viewTables.foreach{
        record =>
          //增加曝光小时总PV
          val view_pv_by_hour_key = "view_pv_" + record.Time.substring(0,13)
          p1.incr(view_pv_by_hour_key)

          //增加曝光小时UV
          val view_uv_by_hour_key: String = "view_pv_" + record.Time.substring(0,13)
          p1.sadd(view_uv_by_hour_key,record.user_id)

          //增加曝光一天总PV
          val view_pv_by_day_key: String = "view_pv_" + record.Time.substring(0,10)
          p1.incr(view_pv_by_day_key)

          //增加曝光一天UV
          val view_uv_by_day_key: String = "view_pv_" + record.Time.substring(0,10)
          p1.sadd(view_uv_by_day_key,record.user_id)
          println(record.Time + "view pv uv has writed")
      }
//      //提交事务
//      p1.exec()
//      //关闭pipeline
//      p1.sync()
      jedis.close()
    })

    //点击的PV UV
    clickDS.foreachRDD(x=>{
      val jedis: Jedis =  RedisUtil.getJedisClient
      val p1: Pipeline = jedis.pipelined()
//      //开启事务
//      p1.multi()
      val clickTables: Array[ClickTable] = x.collect()
      clickTables.foreach{
        record =>
          //增加曝光小时总PV
          val click_pv_by_hour_key: String = "click_pv_" + record.Time.substring(0,13)
          p1.incr(click_pv_by_hour_key)

          //增加曝光小时UV
          val click_uv_by_hour_key: String = "click_pv_" + record.Time.substring(0,13)
          p1.sadd(click_uv_by_hour_key,record.user_id)

          //增加曝光一天总PV
          val click_pv_by_day_key: String = "click_pv_" + record.Time.substring(0,10)
          p1.incr(click_pv_by_day_key)

          //增加曝光一天UV
          val click_uv_by_day_key: String = "click_pv_" + record.Time.substring(0,10)
          p1.sadd(click_uv_by_day_key,record.user_id)
          println(record.Time + "click pv uv has writed")
      }
//      //提交事务
//      p1.exec()
//      //关闭pipeline
//      p1.sync()
      jedis.close()
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(false,true)
  }
}
