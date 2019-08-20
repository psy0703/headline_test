package recommader

/**
  * @Author: Cedaris
  * @Date: 2019/8/15 17:05
  */
import java.lang

import bean._
import caseclass._
import com.alibaba.fastjson.JSON
import common.HBaseUtils._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author: Cedaris
  * @Date: 2019/8/13 15:47
  */
object StreamingTestV2 {
  def main(args: Array[String]): Unit = {

    //spark conf 配置
    val batch = 10
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("StreamingTestV2")
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
      .set("spark.debug.maxToStringFields", "100")
      .setIfMissing("spark.master", "local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "20000")

    //Spark streaming 配置
    val ssc = new StreamingContext(sparkConf, Seconds(batch))
    System.setProperty("HADOOP_USER_NAME", "psy831")
    ssc.checkpoint("hdfs://psy831:9000/checkpoint/StreamingTestV2")

    //定义kafka参数
    val bootstrap: String = "psy831:9092,psy832:9092,psy833:9092"
    val topic: Array[String] = Array("dgmall_log")
    val consumergroup: String = "headline_test03"
//    val partition: Int = 1 //测试topic只有一个分区
//    val start_offset: Long = 0l

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
    val hiveTableName1 = "headlineV2:app_user_actions_info_summary"
    val hiveTableName2 = "headlineV2:app_video_index_info_summary"
    //列族
    val cf1 = "user_actions"
    val cf2 = "video_index"
    val cf3 = "User_Level"
    val cf4 = "video_info"
    //列
    val columnName = "info"


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

      //过滤出click点击日志
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


      val user_videoDf: DataFrame = sql(
        """
        select user_id_1,video_id_1,is_click from view_click
        """.stripMargin)

      user_videoDf.show(20)

      //通过用户id和视频id查询保存在hbase中的离线指标信息
      user_videoDf.as[UserVideo]
        .coalesce(5)
      .foreachPartition(partition => {
      val conn = getHBaseConnection()
      val admin = conn.getAdmin.asInstanceOf[HBaseAdmin]

      val tableName1 = TableName.valueOf(hiveTableName1)
      val table1 = conn.getTable(tableName1)

      val tableName2 = TableName.valueOf(hiveTableName2)
      val table2 = conn.getTable(tableName2)

      partition.foreach(
      row => {
      //从HBASE获取用户行为汇总信息
      val user_actions: String =
      getDataByRowkeyCfColumn(admin, hiveTableName1, row.user_id_1, cf1, columnName)

      //从HBASE获取视频指标汇总信息
      val video_summary: String =
      getDataByRowkeyCfColumn(admin, hiveTableName2, row.video_id_1, cf2, columnName)

      //从HBASE获取用户等级信息
      val user_level: String =
      getDataByRowkeyCfColumn(admin, hiveTableName1, row.user_id_1, cf3, columnName)

      //从HBASE获取视频信息
      val video_info: String =
      getDataByRowkeyCfColumn(admin, hiveTableName2, row.video_id_1, cf4, columnName)

      if (user_actions != null) {
      val uactions: AppUserActions = JSON.parseObject(user_actions,
      classOf[AppUserActions])
      println(uactions.getTime)
      }

      /*if (user_actions != null && video_summary != null && user_level!= null && video_info!= null) {
      val userActions: AppUserActions = ParseObject.parseUserActions(user_actions)
      val videoIndex: AppVideoIndex = ParseObject.parseVideoIndex(video_summary)
      val userInfo: AppUserInfo = ParseObject.parseUserInfo(user_level)
      val videoInfo: AppReleaseVideo = ParseObject.parseVideoInfo(video_info)

      val modelFeatures = ModelFeatures(row.user_id_1.toLong,
      row.is_click,
      "深圳",
      userInfo.getValue_type,
      userInfo.getFrequence_type,
      mkArrayLong(parse2Array(userActions.getCate1_prefer), 5, "-1"),
      mkArrayLong(parse2Array(userActions.getCate2_prefer), 5, "-1"),
      mkArrayDouble(parse2Array(userActions.getWeights_cate1_prefer), 5, "0"),
      mkArrayDouble(parse2Array(userActions.getWeights_cate2_prefer), 5, "0"),
      videoInfo.getVideo_child_tag.toLong,
      videoIndex.getCtr_1day,
      videoIndex.getUv_ctr_1day,
      videoIndex.getPlay_long_1day,
      videoIndex.getPlay_times_1day,
      videoIndex.getCtr_1week,
      videoIndex.getUv_ctr_1week,
      videoIndex.getPlay_long_1week,
      videoIndex.getPlay_times_1week,
      videoIndex.getCtr_2week,
      videoIndex.getUv_ctr_2week,
      videoIndex.getPlay_long_2week,
      videoIndex.getPlay_times_2week,
      videoIndex.getCtr_1month,
      videoIndex.getUv_ctr_1month,
      videoIndex.getPlay_long_1month,
      videoIndex.getPlay_times_1month,
      0.0,
      0.0,
      0.0,
      0.0,
      0.0,
      0.0,
      "0",
      0.0,
      0.0,
      "0",
      "HW",
      "1920*1080",
      userActions.getTimesincelastwatchsqrt.toDouble,
      userActions.getTimesincelastwatch.toDouble,
      userActions.getTimesincelastwatchsquare.toDouble,
      mkArrayLong(parse2Array(userActions.getBehaviorcids), 10, "-1"),
      mkArrayLong(parse2Array(userActions.getBehaviorc1ids), 10, "-1"),
      mkArrayLong(parse2Array(userActions.getBehavioraids), 10, "-1"),
      mkArrayLong(parse2Array(userActions.getBehaviorvids), 10, "-1"),
      mkArrayLong(parse2Array(userActions.getBehaviortokens), 10, "-1"),
      "00001".toLong,
      "9999".toLong,
      "60".toLong,
      "22".toLong
      )
      println(modelFeatures.behaviorAids)
      }*/

      })
      table1.close()
      table2.close()
      conn.close()
      })

//      ModelFeaturesDS.show(10)
//        .write
//        .format("tfrecords")
//        .option("recordType", "Example")
//        .save("hdfs://dev-node02:9000/user/sample/tfRecord")

    })

    ssc.start()
    ssc.awaitTermination()
//    ssc.stop(false, true)
  }

  //将字符串解析为字符串数组
  def parse2Array(str: String): Array[String] = {
    //"WrappedArray(0, 0, 0, 0, 0, 0, 118, 0, 0, 0)"
    str.substring(13,str.length-1).split(",")

  }


  /**
    * 返回指定长度的Array[String]
    *
    * @param arr
    * @param lenth     期待的新数组的长度
    * @param expectStr 长度不足时期待插入的元素
    * @return
    */
  def mkArrayString(arr: Array[String], lenth: Int, expectStr: String)
  : Array[String] = {
    if (arr.length == 0 && arr.length >= lenth) {
      arr
    } else {
      var newArr = new Array[String](lenth)
      newArr ++= arr
      for (i <- 0 to lenth - arr.length - 1) {
        newArr :+ expectStr
      }
      newArr
    }
  }

  /**
    * 返回指定长度的Array[BigInt]
    *
    * @param arr
    * @param lenth     期待的新数组的长度
    * @param expectStr 长度不足时期待插入的元素
    * @return
    */
  def mkArrayLong(arr: Array[String], lenth: Int, expectStr: String): Array[Long] = {
    var newArr = new Array[Long](lenth)
    if (arr.length == 0 && arr.length >= lenth) {
      for (ele <- arr) {
        newArr :+ ele.toLong
      }
    } else {
      for (ele <- arr) {
        newArr :+ ele.toLong
      }

      for (i <- 0 to lenth - arr.length - 1) {
        newArr :+ expectStr.toLong
      }
    }
    newArr
  }

  /**
    * 返回指定长度的Array[Double]
    *
    * @param arr
    * @param lenth     期待的新数组的长度
    * @param expectStr 长度不足时期待插入的元素
    * @return
    */
  def mkArrayDouble(arr: Array[String], lenth: Int, expectStr: String)
  : Array[Double] = {
    var newArr = new Array[Double](lenth)
    if (arr.length == 0 && arr.length >= lenth) {
      for (ele <- arr) {
        newArr :+ ele.toDouble
      }
    } else {

      for (ele <- arr) {
        newArr :+ ele.toDouble
      }

      for (i <- 0 to lenth - arr.length - 1) {
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


//.foreachPartition(partition => {
//val conn = getHBaseConnection()
//val admin = conn.getAdmin.asInstanceOf[HBaseAdmin]
//
//val tableName1 = TableName.valueOf(hiveTableName1)
//val table1 = conn.getTable(tableName1)
//
//val tableName2 = TableName.valueOf(hiveTableName2)
//val table2 = conn.getTable(tableName2)
//
//partition.foreach(
//row => {
////从HBASE获取用户行为汇总信息
//val user_actions: String =
//getDataByRowkeyCfColumn(admin, hiveTableName1, row.user_id_1, cf1, columnName)
//
////从HBASE获取视频指标汇总信息
//val video_summary: String =
//getDataByRowkeyCfColumn(admin, hiveTableName2, row.video_id_1, cf2, columnName)
//
////从HBASE获取用户等级信息
//val user_level: String =
//getDataByRowkeyCfColumn(admin, hiveTableName1, row.user_id_1, cf3, columnName)
//
////从HBASE获取视频信息
//val video_info: String =
//getDataByRowkeyCfColumn(admin, hiveTableName2, row.video_id_1, cf4, columnName)
//
//if (user_actions != null) {
//val uactions: AppUserActions = JSON.parseObject(user_actions,
//classOf[AppUserActions])
//println(uactions.getTime)
//}
//
//if (user_actions != null && video_summary != null && user_level!= null && video_info!= null) {
//val userActions: AppUserActions = ParseObject.parseUserActions(user_actions)
//val videoIndex: AppVideoIndex = ParseObject.parseVideoIndex(video_summary)
//val userInfo: AppUserInfo = ParseObject.parseUserInfo(user_level)
//val videoInfo: AppReleaseVideo = ParseObject.parseVideoInfo(video_info)
//
//val modelFeatures = ModelFeatures(row.user_id_1.toLong,
//row.is_click,
//"深圳",
//userInfo.getValue_type,
//userInfo.getFrequence_type,
//mkArrayLong(parse2Array(userActions.getCate1_prefer), 5, "-1"),
//mkArrayLong(parse2Array(userActions.getCate2_prefer), 5, "-1"),
//mkArrayDouble(parse2Array(userActions.getWeights_cate1_prefer), 5, "0"),
//mkArrayDouble(parse2Array(userActions.getWeights_cate2_prefer), 5, "0"),
//videoInfo.getVideo_child_tag.toLong,
//videoIndex.getCtr_1day,
//videoIndex.getUv_ctr_1day,
//videoIndex.getPlay_long_1day,
//videoIndex.getPlay_times_1day,
//videoIndex.getCtr_1week,
//videoIndex.getUv_ctr_1week,
//videoIndex.getPlay_long_1week,
//videoIndex.getPlay_times_1week,
//videoIndex.getCtr_2week,
//videoIndex.getUv_ctr_2week,
//videoIndex.getPlay_long_2week,
//videoIndex.getPlay_times_2week,
//videoIndex.getCtr_1month,
//videoIndex.getUv_ctr_1month,
//videoIndex.getPlay_long_1month,
//videoIndex.getPlay_times_1month,
//0.0,
//0.0,
//0.0,
//0.0,
//0.0,
//0.0,
//"0",
//0.0,
//0.0,
//"0",
//"HW",
//"1920*1080",
//userActions.getTimesincelastwatchsqrt.toDouble,
//userActions.getTimesincelastwatch.toDouble,
//userActions.getTimesincelastwatchsquare.toDouble,
//mkArrayLong(parse2Array(userActions.getBehaviorcids), 10, "-1"),
//mkArrayLong(parse2Array(userActions.getBehaviorc1ids), 10, "-1"),
//mkArrayLong(parse2Array(userActions.getBehavioraids), 10, "-1"),
//mkArrayLong(parse2Array(userActions.getBehaviorvids), 10, "-1"),
//mkArrayLong(parse2Array(userActions.getBehaviortokens), 10, "-1"),
//"00001".toLong,
//"9999".toLong,
//"60".toLong,
//"22".toLong
//)
//println(modelFeatures.behaviorAids)
//}
//
//
//
//})
//table1.close()
//table2.close()
//conn.close()
//
//})