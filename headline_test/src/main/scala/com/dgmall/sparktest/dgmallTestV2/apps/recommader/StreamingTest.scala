package com.dgmall.sparktest.dgmallTestV2.apps.recommader

import java.lang
import com.alibaba.fastjson.JSON
import com.dgmall.sparktest.dgmallTestV2.bean.{AppClick, AppView}
import com.dgmall.sparktest.dgmallTestV2.caseclass.{ClickTable, CommenRecord, ViewTable}
import com.dgmall.sparktest.dgmallTestV2.common.ParseObject
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * @Author: Cedaris
  * @Date: 2019/8/13 15:47
  */
object StreamingTest {
  def main(args: Array[String]): Unit = {
    val batch = 10
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("pv_uv")
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
      .set("spark.debug.maxToStringFields", "100")
      .setIfMissing("spark.master", "local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "20000")

    val ssc = new StreamingContext(sparkConf, Seconds(batch))
    System.setProperty("HADOOP_USER_NAME", "psy831")
    ssc.checkpoint("hdfs://psy831:9000/checkpoint/headline_test")

    //定义kafka参数
    val bootstrap: String = "psy831:9092,psy832:9092,psy833:9092"
    val topic: Array[String] = Array("dgmall_log")
    val consumergroup: String = "headline_test813"
    val partition: Int = 1 //测试topic只有一个分区
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

    recordDs.foreachRDD(x=>{
      val spark = SparkSessionSingleton.getInstance(x.sparkContext.getConf)
      import spark.implicits._
      import spark.sql

      val viewDF: DataFrame = x.filter(y => if (y.Event.contains("view")) true else false)
        .map(x => {
          val properties: String = x.Properties.toString
          val view: AppView = JSON.parseObject(properties, classOf[AppView])
          //      val words: Array[String] = view.getTrace_id.split(".")
          //转换为曝光表结构的类
          ViewTable(x.distinct_id, x.Time, x.Event, x.Type,
            view.getUser_id, view.getVideo_id, view.getTrace_id
          )
        }).toDF()
      viewDF.createOrReplaceTempView("tb_view")

      val clickDF: DataFrame = x.filter(y => if (y.Event.contains("click")) true else false)
        .map(x => {
          val properties: String = x.Properties.toString
          val click: AppClick = JSON.parseObject(properties, classOf[AppClick])
          //      val words: Array[String] = click.getTrace_id.split(".")
          //转换为点击表结构的类
          ClickTable(x.distinct_id, x.Time, x.Event, x.Type,
            click.getOrder, click.getTrace_id, click.getUser_id, click.getVideo_id
          )
        }).toDF()
      clickDF.createOrReplaceTempView("tb_click")

      sql(
        """
          |select
          |user_id,
          |video_id,
          |concat(user_id , '-', video_id) as u_vcode
          |from tb_view
        """.stripMargin).createOrReplaceTempView("tb_view2")

      sql(
        """
          |select
          |user_id,
          |video_id,
          |concat(user_id , '-', video_id) as u_vcode
          |from tb_click
        """.stripMargin).createOrReplaceTempView("tb_click2")

      sql(
        """
          |select
          |t1.user_id,
          |t1.video_id,
          |t2.user_id,
          |t2.video_id,
          |if(t2.video_id is null,0,1) as is_click
          |from
          |tb_view2 t1
          |left join tb_click2 t2
          |on t1.u_vcode = t2.u_vcode
        """.stripMargin).createOrReplaceTempView("view_click")



      sql(
        """
          |select * from view_click
        """.stripMargin).show()
    })


    //todo: 拼接HIVE离线数据




    ssc.start()
    ssc.awaitTermination()
    ssc.stop(false,true)
  }

  object SparkSessionSingleton{
    @transient  private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }
}
