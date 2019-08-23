package recommader

import java.lang
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import bean.{AppClick, AppView}
import caseclass.{ClickTable, CommenRecord, ViewTable}
import common.CommonUtils.SparkSessionSingleton
import common.{InternalRedisClient, KafkaSink}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * 将KAFKA 中的曝光、点击数据读出，转化为 user_id + video_id + is_click（是否点击）
  * json字符串写入到 KAFKA的新topic  ——  user_video_click
  * @Author: Cedaris
  * @Date: 2019/8/20 10:21
  */
object Kafka2KafkaStreaming {

  private val LOG = LoggerFactory.getLogger("Kafka2KafkaStreaming")
  private val STOP_FLAG = "TEST_STOP_FLAG"

  def main(args: Array[String]): Unit = {
    //初始化Redis Pool
    initRedisPool()

    val conf = new SparkConf()
      .setAppName("Kafka2KafkaStreaming")
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
      .set("spark.debug.maxToStringFields", "100")
      .setIfMissing("spark.master", "local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "20000")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    System.setProperty("HADOOP_USER_NAME", "dev")

    val ssc = new StreamingContext(sc, Seconds(10))
    ssc.checkpoint("hdfs://dev-node02:9000/spark/checkpoint/kafka-streaming")

    //    val bootstrapServers = "psy831:9092,psy832:9092,psy833:9092"
    val bootstrapServers = "dev-node01:9092,dev-node02:9092,dev-node03:9092"
    val groupId = "kafka-kafka"
    val topicName = "dgmall_log"
    val topic: Array[String] = Array("dgmall_log")
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

    //通过KafkaUtil创建KafkaDStream
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      PreferConsistent,
      Subscribe[String, String](topic, kafkaParams)
    )

    /*// 这里指定Topic的Partition的总数
    val fromOffsets = getLastCommittedOffsets(topicName, 3)
    // 初始化KafkaDS
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets))*/


    // 初始化KafkaSink,并广播
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", bootstrapServers)
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      if (LOG.isInfoEnabled)
        LOG.info("kafka producer init done!")
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    //读取kafka的数据并转化为日志格式
    stream.foreachRDD(
      rdd => {
//        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        import spark.implicits._
        import spark.sql

        // 如果rdd有数据
        if (!rdd.isEmpty()) {
          val commenRecord: RDD[CommenRecord] = rdd.map(y => {
            val records: String = y.value()
            val json: CommenRecord = JSON.parseObject(records, classOf[CommenRecord])
            json
          })
          //过滤出view曝光日志
          val viewDF: DataFrame = commenRecord.filter(y => if (y.Event.contains("view")) true else false)
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
          val clickDF: DataFrame = commenRecord.filter(y => if (y.Event.contains("click")) true else false)
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
            .foreachPartition(partition => {

              /*val jedis = InternalRedisClient.getPool.getResource
              val p = jedis.pipelined()
              p.multi() //开启事务*/

              partition.foreach(row => {
                //创建json对象
                val json = new JSONObject()
                json.put("user_id", row.user_id_1)
                json.put("video_id", row.video_id_1)
                json.put("is_click", row.is_click)

                //发送到kafka指定的topic
                kafkaProducer.value.send("user_video_click", json.toJSONString)
              })
              /*val offsetRange = offsetRanges(TaskContext.get.partitionId)
              println("partition : " + offsetRange.partition + " fromOffset:  " + offsetRange.fromOffset + " untilOffset: " + offsetRange.untilOffset)
              val topic_partition_key = offsetRange.topic + "_" + offsetRange.partition
              p.set(topic_partition_key, offsetRange.untilOffset + "")

              p.exec() //提交事务
              p.sync //关闭pipeline
              jedis.close()*/
            })
        }
      })
    ssc.start()
    // 优雅停止
    stopByMarkKey(ssc)
    ssc.awaitTermination()
  }

  /**
    * 优雅停止
    * @param ssc
    */
  def stopByMarkKey(ssc: StreamingContext): Unit = {

    val intervalMills = 10 * 1000 // 每隔10秒扫描一次消息是否存在
    var isStop = false
    while (!isStop) {
      isStop = ssc.awaitTerminationOrTimeout(intervalMills)
      if (!isStop && isExists(STOP_FLAG)) {
        LOG.warn("2秒后开始关闭sparstreaming程序.....")
        Thread.sleep(2000)
        ssc.stop(true, true)
      }
    }
  }

  def initRedisPool() = {
    // Redis configurations
    val maxTotal = 20
    val maxIdle = 10
    val minIdle = 1
    val redisHost = "127.0.0.1"
    val redisPort = 6379
    val redisTimeout = 30000
    InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)
  }

  /**
    * 从redis里获取Topic的offset值
    * @param topicName
    * @param partitions
    * @return
    */
  def getLastCommittedOffsets(topicName: String, partitions: Int): Map[TopicPartition, Long] = {
    if (LOG.isInfoEnabled())
      LOG.info("||--Topic:{},getLastCommittedOffsets from Redis--||", topicName)

    //从Redis获取上一次存的Offset
    val jedis = InternalRedisClient.getPool.getResource
    val fromOffsets = collection.mutable.HashMap.empty[TopicPartition, Long]
    for (partition <- 0 to partitions - 1) {
      val topic_partition_key = topicName + "_" + partition
      val lastSavedOffset = jedis.get(topic_partition_key)
      val lastOffset = if (lastSavedOffset == null) 0L else lastSavedOffset.toLong
      fromOffsets += (new TopicPartition(topicName, partition) -> lastOffset)
    }
    jedis.close()

    fromOffsets.toMap
  }

  /**
    * 判断Key是否存在
    * @param key
    * @return
    */
  def isExists(key: String): Boolean = {
    val jedis = InternalRedisClient.getPool.getResource
    val flag = jedis.exists(key)
    jedis.close()
    flag
  }

  //用户id和视频id 样例类
  case class UserVideo(user_id_1: String, video_id_1: String, is_click: Int) {}

}
