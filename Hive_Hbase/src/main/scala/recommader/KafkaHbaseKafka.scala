package recommader

import java.lang
import common.CommonUtils._
import common.HBaseUtils._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * @Author: Cedaris
  * @Date: 2019/8/20 11:51
  */
object KafkaHbaseKafka {
  private val LOG = LoggerFactory.getLogger("KafkaHbaseKafka")
  private val STOP_FLAG = "TEST_STOP_FLAG"

  def main(args: Array[String]): Unit = {

    //配置spark conf
    val conf = new SparkConf()
      .setAppName("Kafka2KafkaStreaming")
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
      .set("spark.debug.maxToStringFields", "100")
      .setIfMissing("spark.master", "local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "20000")
      .set("spark.serializer","org.apache.spark.serializer.JavaSerializer")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    System.setProperty("HADOOP_USER_NAME", "dev")

    //配置spark Streaming
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("hdfs://dev-node02:9000/spark/checkpoint/KafkaHbaseKafka")

    val bootstrapServers = "dev-node01:9092,dev-node02:9092,dev-node03:9092"
    val groupId = "kafka-hbase"
    val topicName = "user_video_click"
    val topic: Array[String] = Array("user_video_click")
    val maxPoll = 1000

    //将kafka参数映射为map
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> bootstrapServers,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: lang.Boolean)
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


    //通过KafkaUtil创建KafkaDStream
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      PreferConsistent,
      Subscribe[String, String](topic, kafkaParams)
    )

    kafkaStream
      .foreachRDD(rdd => {
      // 如果rdd有数据
      if (!rdd.isEmpty()){
        //todo json  转 uvc
        rdd.foreachPartition(partition => {
          val conn = getHBaseConnection()
          val admin = conn.getAdmin.asInstanceOf[HBaseAdmin]

          val tableName1 = TableName.valueOf(hiveTableName1)
          val table1 = conn.getTable(tableName1)

          val tableName2 = TableName.valueOf(hiveTableName2)
          val table2 = conn.getTable(tableName2)

          partition.foreach(row => {
            //取出KAFKA中的数据
            val jsonStr: String = row.value() //将数据格式由json字符串转为用户视频点击对象
//            val userVideoClick: UserVideo = JSON.parseObject(jsonStr,classOf[UserVideo])
//            userVideoClick
            println(jsonStr)
          })
           /* .foreach(
            userVideoClick => {
              println(userVideoClick.video_id_1 + userVideoClick.user_id_1 + userVideoClick.is_click)
              /*//从HBASE获取用户行为汇总信息
              val user_actions: String =
                getDataByRowkeyCfColumn(admin, hiveTableName1, userVideoClick.user_id_1, cf1, columnName)

              //从HBASE获取视频指标汇总信息
              val video_summary: String =
                getDataByRowkeyCfColumn(admin, hiveTableName2, userVideoClick.video_id_1, cf2, columnName)

              //从HBASE获取用户等级信息
              val user_level: String =
                getDataByRowkeyCfColumn(admin, hiveTableName1, userVideoClick.user_id_1, cf3, columnName)

              //从HBASE获取视频信息
              val video_info: String =
                getDataByRowkeyCfColumn(admin, hiveTableName2, userVideoClick.video_id_1, cf4, columnName)


              val uactions: AppUserActions = JSON.parseObject(user_actions,
                classOf[AppUserActions])
              println(uactions.getUser_id + "\t" + uactions.getTime + "\t" + uactions.getBehavioraids)*/

             /* if (user_actions != null && video_summary != null && user_level!= null && video_info!= null) {
                val userActions: AppUserActions = ParseObject.parseUserActions(user_actions)
                val videoIndex: AppVideoIndex = ParseObject.parseVideoIndex(video_summary)
                val userInfo: AppUserInfo = ParseObject.parseUserInfo(user_level)
                val videoInfo: AppReleaseVideo = ParseObject.parseVideoInfo(video_info)

                val modelFeatures = ModelFeatures(userVideoClick.getUser_id.toLong,
                  userVideoClick.getIs_click.toInt,
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

            }
          )*/

          table1.close()
          table2.close()
          conn.close()
        })

      }
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(false, true)
  }
  //用户id和视频id 样例类
  case class UserVideo(user_id_1: String, video_id_1: String, is_click: Int) {}

}

