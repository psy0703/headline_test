package recommader

/**
  * @Author: Cedaris
  * @Date: 2019/8/15 17:05
  */
import java.lang
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import bean._
import caseclass._
import common.HBaseUtils._
import common.CommonUtils._
import common._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory


object StreamingTestV2 {

  private val LOG = LoggerFactory.getLogger("StreamingTestV2")
  private val STOP_FLAG = "TEST_STOP_FLAG"

  def main(args: Array[String]): Unit = {

    //spark conf 配置
    val batch = 10
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("StreamingTestV3")
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
      .set("spark.debug.maxToStringFields", "100")
      .setIfMissing("spark.master", "local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "20000")
      .set("spark.driver.memory", "1024M")

    //Spark streaming 配置
    val ssc = new StreamingContext(sparkConf, Seconds(batch))
    System.setProperty("HADOOP_USER_NAME", "dev")
    ssc.checkpoint("hdfs://dev-node02:9000/spark/checkpoint/StreamingTest")

    //定义kafka参数
    val bootstrap: String = "dev-node01:9092,dev-node02:9092,dev-node03:9092"
    val topic: Array[String] = Array("dgmall_log")
    val consumergroup: String = "headline_test04"
//    val partition: Int = 1 //测试topic只有一个分区
//    val start_offset: Long = 0l

    // 初始化KafkaSink,并广播
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", bootstrap)
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      if (LOG.isInfoEnabled)
        LOG.info("kafka producer init done!")
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }


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
    val hiveTableName3 = "headlineV2:app_video_info_summary"
    //列族
    val cf1 = "user_actions"
    val cf2 = "video_index"
    val cf3 = "User_Info"
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


      val user_videoDf: DataFrame = sql(
        """
        select user_id_1,video_id_1,is_click from view_click
        """.stripMargin)

      user_videoDf.show(20)

      //通过用户id和视频id查询保存在hbase中的离线指标信息
      user_videoDf.as[UserVideo]
        .repartition(5)
        .foreachPartition(partition => {
          val conn = getHBaseConnection()
          val admin = conn.getAdmin.asInstanceOf[HBaseAdmin]

          val tableName1 = TableName.valueOf(hiveTableName1)
          val table1 = conn.getTable(tableName1)

          val tableName2 = TableName.valueOf(hiveTableName2)
          val table2 = conn.getTable(tableName2)

          partition.foreach(
            row => {
              println(row.user_id_1 + "\t" + row.video_id_1 + "\t" + row.is_click)
              //从HBASE获取用户行为汇总信息
              var user_actions: String =
                getDataByRowkeyCfColumn(admin, hiveTableName1, row.user_id_1, cf1, columnName)

              //从HBASE获取视频指标汇总信息
              var video_summary: String =
                getDataByRowkeyCfColumn(admin, hiveTableName2, row.video_id_1, cf2, columnName)

              //从HBASE获取用户等级信息
              var user_info: String =
                getDataByRowkeyCfColumn(admin, hiveTableName1, row.user_id_1, cf3, columnName)

              //从HBASE获取视频信息
              var video_info: String =
                getDataByRowkeyCfColumn(admin, hiveTableName3, row.video_id_1, cf4, columnName)

              //todo 调试专用 hbase中读取到数据为null时 ，采用默认数据
              if (user_actions == null) {
                val default_userAction = "{\"weights_cate1_prefer\":\"WrappedArray(1.0)\",\"weights_cate2_prefer\":\"WrappedArray(1.0)\",\"watch_last_time\":\"2019-08-13 16:08:03\",\"timesincelastwatchsqrt\":\"395.29\",\"behaviortokens\":\"WrappedArray(炭你项矮狐, 训穗特医肄, 哇瘁蛾高淌, 乞酱刽伍基, 萎曼职般疾, 挑秤邮遭累, 铂惦卸恤眶, 摹刁莹悔午, 余铣峦浦芍, 薯萧靳捂文)\",\"cate1_prefer\":\"WrappedArray(25)\",\"cate2_prefer\":\"WrappedArray(93)\",\"timesincelastwatch\":\"156256.0\",\"behaviorcids\":\"WrappedArray(25, 0, 0, 0, 0, 0, 0)\",\"behaviorc1ids\":\"WrappedArray(93, 0, 0, 0, 0, 0, 0)\",\"timesincelastwatchsquare\":\"2.4415937536E10\",\"behaviorvids\":\"WrappedArray(76907, 15441, 43287, 61437, 15640, 56980, 29633)\",\"user_id\":\"07924\",\"time\":\"2019-08-15 11:32:19\",\"behavioraids\":\"WrappedArray(83483, 0, 0, 0, 0, 0, 0)\"}"
                user_actions = default_userAction
              }

              if (video_summary == null) {
                val defalut_videoSummary = "{\"uv_ctr_1day\":\"0.0\",\"uv_ctr_1week\":\"0.429\",\"play_times_2week\":\"2.0\",\"play_long_2week\":\"171.0\",\"play_times_1day\":\"0.0\",\"ctr_1day\":\"0.0\",\"ctr_1week\":\"0.429\",\"play_times_1week\":\"2.0\",\"ctr_2week\":\"0.429\",\"play_long_1week\":\"171.0\",\"uv_ctr_1month\":\"0.429\",\"play_times_1month\":\"2.0\",\"uv_ctr_2week\":\"0.429\",\"play_long_1month\":\"171.0\",\"ctr_1month\":\"0.429\",\"video_id\":\"49637\",\"play_long_1day\":\"0.0\"}"
                video_summary = defalut_videoSummary
              }
              if (user_info == null) {
                val default_userInfo = "{\"sum_play_long\":\"129.0\",\"value_type\":\"L02\",\"frequence_type\":\"L02\",\"user_id\":\"68852\",\"sum_play_times\":\"3\",\"play_times_rank\":\"71997\",\"play_long_rank\":\"72252\"}"
                user_info = default_userInfo
              }
              if (video_info == null) {
                val default_videoInfo = "{\"upload_time\":\"2019-08-15 18:01:02\",\"video_child_tag\":\"101\",\"music_write\":\"xxx\",\"video_address\":\"xxx\",\"user_id\":\"01341\",\"video_topic\":\"体育\",\"music_name\":\"xxx\",\"video_long\":\"871\",\"video_tag\":\"18\",\"video_id\":\"65842\",\"video_desc\":\"xxx\"}"
                video_info = default_videoInfo
              }

              println(user_actions)
              println(video_summary)
              println(user_info)
              println(video_info)

              val userActions: AppUserActions = ParseObject.parseUserActions(user_actions)
              val videoIndex: AppVideoIndex = ParseObject.parseVideoIndex(video_summary)
              val userInfo: AppUserInfo = ParseObject.parseUserInfo(user_info)
              val videoInfo: AppReleaseVideo = ParseObject.parseVideoInfo(video_info)


              //创建json对象
              val json = new JSONObject()

              json.put("audience_id", row.user_id_1)
              json.put("item_id", row.video_id_1)
              json.put("click", row.is_click.toString)
              json.put("city", "深圳")
              json.put("value_type", userInfo.getValue_type)
              json.put("frequence_type",userInfo.getFrequence_type)
              json.put("cate1_prefer", userActions.getCate1_prefer)
              json.put("cate2_prefer", userActions.getCate2_prefer)
              json.put("weights_cate1_prefer", userActions.getWeights_cate1_prefer)
              json.put("weights_cate2_prefer", userActions.getWeights_cate2_prefer)
              json.put("cate2Id", videoInfo.getVideo_child_tag)
              json.put("ctr_1d", videoIndex.getCtr_1day.toString)
              json.put("uv_ctr_1d", videoIndex.getUv_ctr_1day.toString)
              json.put("play_long_1d", videoIndex.getPlay_long_1day.toString)
              json.put("play_times_1d", videoIndex.getPlay_times_1day.toString)
              json.put("ctr_1w", videoIndex.getCtr_1week.toString)
              json.put("uv_ctr_1w", videoIndex.getUv_ctr_1week.toString)
              json.put("play_long_1w", videoIndex.getPlay_long_1week.toString)
              json.put("play_times_1w", videoIndex.getPlay_times_1week.toString)
              json.put("ctr_2w", videoIndex.getCtr_2week.toString)
              json.put("uv_ctr_2w",videoIndex.getUv_ctr_2week.toString)
              json.put("play_long_2w", videoIndex.getPlay_long_2week.toString)
              json.put("play_times_2w", videoIndex.getPlay_times_2week.toString)
              json.put("ctr_1m", videoIndex.getCtr_1month.toString)
              json.put("uv_ctr_1m", videoIndex.getUv_ctr_1month.toString)
              json.put("play_long_1m", videoIndex.getPlay_long_1month.toString)
              json.put("play_times_1m", videoIndex.getPlay_times_1month.toString)
              json.put("matchScore", "0")
              json.put("popScore", "0")
              json.put("exampleAge", "0")
              json.put("cate2Prefer", "0")
              json.put("catePrefer", "0")
              json.put("authorPrefer", "0")
              json.put("position", "1")
              json.put("triggerNum", "0")
              json.put("triggerRank", "0")
              json.put("hour", "0")
              json.put("phoneBrand", "HW")
              json.put("phoneResolution", "1920*1080")
              json.put("timeSinceLastWatchSqrt",userActions.getTimesincelastwatchsqrt.toString)
              json.put("timeSinceLastWatch",  userActions.getTimesincelastwatch.toString)
              json.put("timeSinceLastWatchSquare", userActions.getTimesincelastwatchsquare.toString)
              json.put("behaviorCids", userActions.getBehaviorcids)
              json.put("behaviorC1ids", userActions.getBehaviorc1ids)
              json.put("behaviorAids", userActions.getBehavioraids)
              json.put("behaviorVids", userActions.getBehaviorvids)
              json.put("behaviorTokens", userActions.getBehaviortokens)
              json.put("videoId", "00001")
              json.put("authorId", "9999")
              json.put("cate1Id", "66")
              json.put("cateId","22")

              //发送到kafka指定的topic
              kafkaProducer.value.send("modelFeatures", json.toJSONString)

            })

          table1.close()
          table2.close()
          conn.close()
        })


    })
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(false, true)
  }


  //用户id和视频id 样例类
  case class UserVideo(user_id_1: String, video_id_1: String, is_click: Int) {}
}