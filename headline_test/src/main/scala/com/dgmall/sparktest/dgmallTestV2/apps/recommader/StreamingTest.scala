package com.dgmall.sparktest.dgmallTestV2.apps.recommader

import java.lang

import com.alibaba.fastjson.JSON
import com.dgmall.sparktest.dgmallTestV2.apps.recommader.Hive2Hbase.getHBaseConnection
import com.dgmall.sparktest.dgmallTestV2.bean.{AppClick, AppView, Constants}
import com.dgmall.sparktest.dgmallTestV2.caseclass.{ClickTable, CommenRecord, UserActionsSummay, VideoIndexSummay, ViewTable}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import com.dgmall.sparktest.dgmallTestV2.common.HBaseUtils._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @Author: Cedaris
  * @Date: 2019/8/13 15:47
  */
object StreamingTest {
  def main(args: Array[String]): Unit = {
    val batch = 10
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("StreamingTest")
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
      .set("spark.debug.maxToStringFields", "100")
      .setIfMissing("spark.master", "local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "20000")

    //Spark streaming 配置
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

    //需要读取的hbase 的表
    val hiveTableName1 = "headline:app_user_actions_summary"
    val hiveTableName2 = "headline:app_video_summary"
    val hiveTableName3 = "headline:user_level"
    //列族
    val cf1 = Constants.HBASE_COLUMN_FAMILY
    val day = "2019-08-14"
    val cf2 = day
    val cf3 = "User_Level"

    //app_user_actions_summary
    var user_actions_list = new ListBuffer[String]
    user_actions_list.append("user_id", "time", "watch_last_time", "timesincelastwatch",
      "timesincelastwatchsqrt", "timesincelastwatchsquare", "behaviorvids",
      "behavioraids", "behaviorcids", "behaviorc1ids", "behaviortokens",
      "cate1_prefer", "weights_cate1_prefer", "cate2_prefer", "weights_cate2_prefer")

    //app_video_summary
    var video_summary_list = new ListBuffer[String]
    video_summary_list.append("video_id",
      "ctr_1day", "uv_ctr_1day", "play_long_1day", "play_times_1day",
      "ctr_1week", "uv_ctr_1week", "play_long_1week", "play_times_1week",
      "ctr_2week", "uv_ctr_2week", "play_long_2week", "play_times_2week",
      "ctr_1month", "uv_ctr_1month", "play_long_1month", "play_times_1month")

    var user_level_list = new ListBuffer[String]
    user_level_list.append("user_id", "sum_play_long", "sum_play_times",
      "play_long_rank", "play_times_rank", "value_type", "frequence_type")


    //读取kafka的数据并转化为日志格式
    val recordDs: DStream[CommenRecord] = stream.map(x => {
      val records: String = x.value()
      val json: CommenRecord = JSON.parseObject(records, classOf[CommenRecord])
      json
    })

    recordDs.foreachRDD(x => {
      val spark = SparkSessionSingleton.getInstance(x.sparkContext.getConf)
      import spark.implicits._
      import spark.sql

      //过滤出view曝光日志
      val viewDF: DataFrame = x.filter(y => if ("view".equals(y.Type)) true else false)
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

      //过滤出click点击日志
      val clickDF: DataFrame = x.filter(y => if ("click".equals(y.Type)) true else false)
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

      //转化格式，便于进行join操作
      sql(
        """
          |select
          |user_id,
          |video_id,
          |concat(user_id , '-', video_id) as u_vcode
          |from tb_view
        """.stripMargin).createOrReplaceTempView("tb_view2")
      //转化格式，便于进行join操作
      sql(
        """
          |select
          |user_id,
          |video_id,
          |concat(user_id , '-', video_id) as u_vcode
          |from tb_click
        """.stripMargin).createOrReplaceTempView("tb_click2")

      //view和click日志按照 user_id-video_id 进行jion操作，获取用户是否曝光后点击
      sql(
        """
          |select
          |t1.user_id as user_id_1,
          |t1.video_id as video_id_1,
          |t2.user_id as user_id_2,
          |t2.video_id as video_id_2,
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

      val user_videoDf: DataFrame = sql(
        """
        select user_id_1,video_id_1,is_click from view_click
        """.stripMargin)

      //通过用户id和视频id查询保存在hbase中的离线指标信息
      user_videoDf.as[UserVideo].foreachPartition(partition => {
        val conn = getHBaseConnection()
        val admin = conn.getAdmin.asInstanceOf[HBaseAdmin]

        val tableName1 = TableName.valueOf(hiveTableName1)
        val table1 = conn.getTable(tableName1)

        val tableName2 = TableName.valueOf(hiveTableName2)
        val table2 = conn.getTable(tableName2)

        val tableName3 = TableName.valueOf(hiveTableName3)
        val table3 = conn.getTable(tableName3)

        partition.foreach(row => {
          //从HBASE获取用户行为汇总信息
          val user_actions_map: mutable.HashMap[String, String] =
            getDataByRowkeyCf(admin, hiveTableName1, row.user_id_1, cf1)

          //从HBASE获取视频指标汇总信息
          val video_summary_map: mutable.HashMap[String, String] =
            getDataByRowkeyCf(admin, hiveTableName2, row.video_id_1, cf2)

          //从HBASE获取用户等级信息
          val user_level_map: mutable.HashMap[String, String] =
            getDataByRowkeyCf(admin, hiveTableName3, row.user_id_1, cf3)

          /*user_level_map.getOrElse("user_id", "0")
          user_level_map.getOrElse("value_type", "0")
          user_level_map.getOrElse("frequence_type", "0")*/

          println(hbase2UserActions(user_actions_map).toString)


        })
        table1.close()
        table2.close()
        table3.close()
        conn.close()

      })
    })

    //todo: 拼接HIVE离线数据

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(false, true)
  }

  def hbase2UserActions(map: mutable.HashMap[String, String]): UserActionsSummay = {
    UserActionsSummay(
      map.getOrElse("user_id", "0"),
      map.getOrElse("time", "0"),
      map.getOrElse("watch_last_time", "0"),
      map.getOrElse("timesincelastwatch", 0.0).toString.toDouble,
      map.getOrElse("timesincelastwatchsqrt", 0.0).toString.toDouble,
      map.getOrElse("timesincelastwatchsquare", 0.0).toString.toDouble,
      mkArrayString(map.getOrElse("behaviorvids", 0).toString.split(","),10,"-1"),
      mkArrayString(map.getOrElse("behavioraids", 0).toString.split(","),10,"-1"),
      mkArrayString(map.getOrElse("behaviorcids", 0).toString.split(","),10,"-1"),
      mkArrayString(map.getOrElse("behaviorc1ids", 0).toString.split(","),10,"-1"),
      mkArrayString(map.getOrElse("behaviortokens", 0).toString.split(","),10,"-1"),
      mkArrayString(map.getOrElse("cate1_prefer", 0).toString.split(","),10,"-1"),
      mkArrayDouble(map.getOrElse("weights_cate1_prefer", 0.0).toString.split
      (","),10,"0"),
      mkArrayString(map.getOrElse("cate2_prefer", 0).toString.split(","),10,"-1"),
      mkArrayDouble(map.getOrElse("weights_cate2_prefer", 0.0).toString.split
      (","),10,"0")
    )

  }

  def hbase2VideoSummay(map: mutable.HashMap[String, String]):VideoIndexSummay={
    VideoIndexSummay(
      map.getOrElse("video_id", "0"),
      map.getOrElse("ctr_1day", 0.0).toString.toDouble,
      map.getOrElse("uv_ctr_1day", 0.0).toString.toDouble,
      map.getOrElse("play_long_1day", 0.0).toString.toDouble,
      map.getOrElse("play_times_1day", 0.0).toString.toDouble,
      map.getOrElse("ctr_1week", 0.0).toString.toDouble,
      map.getOrElse("uv_ctr_1week", 0.0).toString.toDouble,
      map.getOrElse("play_long_1week", 0.0).toString.toDouble,
      map.getOrElse("play_times_1week", 0.0).toString.toDouble,
      map.getOrElse("ctr_2week", 0.0).toString.toDouble,
      map.getOrElse("uv_ctr_2week", 0.0).toString.toDouble,
      map.getOrElse("play_long_2week", 0.0).toString.toDouble,
      map.getOrElse("play_times_2week", 0.0).toString.toDouble,
      map.getOrElse("ctr_1month", 0.0).toString.toDouble,
      map.getOrElse("uv_ctr_1month", 0.0).toString.toDouble,
      map.getOrElse("play_long_1month", 0.0).toString.toDouble,
      map.getOrElse("play_times_1month", 0.0).toString.toDouble
    )
  }

  /**
    * 返回指定长度的Array[String]
    * @param arr
    * @param lenth 期待的新数组的长度
    * @param expectStr 长度不足时期待插入的元素
    * @return
    */
  def mkArrayString(arr:Array[String],lenth:Int,expectStr:String)
  :Array[String]={
    if(arr.length == 0 && arr.length >= lenth) {
      arr
    }else{
      var newArr = new Array[String](lenth)
      newArr ++= arr
      for(i <- 0 to lenth - arr.length -1 ){
        newArr :+ expectStr
      }
      newArr
    }
  }

  /**
    * 返回指定长度的Array[Double]
    * @param arr
    * @param lenth 期待的新数组的长度
    * @param expectStr 长度不足时期待插入的元素
    * @return
    */
  def mkArrayDouble(arr:Array[String],lenth:Int,expectStr:String)
  :Array[Double]={
    var newArr = new Array[Double](lenth)
    if(arr.length == 0 && arr.length >= lenth) {
      for(ele <- arr){
        newArr :+ ele.toDouble
      }
    }else{

      for(ele <- arr){
        newArr :+ ele.toDouble
      }

      for(i <- 0 to lenth - arr.length -1 ){
        newArr :+ expectStr.toDouble
      }
    }
    newArr
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
  case class UserVideo(user_id_1: String, video_id_1: String, is_click: Int) {}

}