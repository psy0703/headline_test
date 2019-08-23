package com.dgmall.sparktest.dgmallTestV2.apps.recommader

import java.io.File
import java.lang
import com.alibaba.fastjson.JSON
import com.dgmall.sparktest.dgmallTestV2.common.CommonUtils._
import com.dgmall.sparktest.dgmallTestV2.bean.AppModelFeatures
import com.dgmall.sparktest.dgmallTestV2.caseclass.ModelFeatures
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark 实时写数据到Hive
  *
  * @Author: Cedaris
  * @Date: 2019/8/23 14:27
  */
object Streaming2Hive {
  //  private val LOG = LoggerFactory.getLogger("Streaming2Hive")
  //  private val STOP_FLAG = "TEST_STOP_FLAG"

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.INFO)
    Logger.getLogger("org.apache.kafka.clients.consumer").setLevel(Level.INFO)

    System.setProperty("HADOOP_USER_NAME", "dev")

    val warehouseLocation = new File("hdfs://dev-node02:9000/user/hive/warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .appName("Streaming2Hive")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    spark.conf.set("spark.streaming.kafka.maxRatePerPartition", "2000")
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    spark.conf.set("spark.streaming.concurrentJobs", "10")
    spark.conf.set("spark.streaming.kafka.maxRetries", "50")


    @transient
    val sc = spark.sparkContext
    //    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(10))
    //kafka参数
    val bootstrapServers = "dev-node01:9092,dev-node02:9092,dev-node03:9092"
    val groupId = "kafka-hive"
    val topic: Array[String] = Array("modelFeatures")
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
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      PreferConsistent,
      Subscribe[String, String](topic, kafkaParams)
    )

    val modelDS: DStream[AppModelFeatures] = kafkaStream.map(record => {
      val jsonStr: String = record.value()
      val modelFeatures: AppModelFeatures = JSON.parseObject(jsonStr, classOf[AppModelFeatures])
      modelFeatures
    })

    modelDS.foreachRDD(rdd => {

      val modelRDD: RDD[ModelFeatures] = rdd.map(y => {
        ModelFeatures(
          y.getAudience_id.toLong,
          y.getItem_id.toLong,
          y.getClick.toInt,
          y.getCity,
          y.getValue_type,
          y.getFrequence_type,
          mkArrayLong(parse2Array(y.getCate1_prefer), 5, "-1"),
          mkArrayString(parse2Array(y.getCate2_prefer), 5, "-1"),
          mkArrayDouble(parse2Array(y.getWeights_cate1_prefer), 5, "0"),
          mkArrayDouble(parse2Array(y.getWeights_cate2_prefer), 5, "0"),
          y.getCate2Id,
          y.getCtr_1d.toDouble,
          y.getUv_ctr_1d.toDouble,
          y.getPlay_long_1d.toDouble,
          y.getPlay_times_1d.toDouble,
          y.getCtr_1w.toDouble,
          y.getUv_ctr_1w.toDouble,
          y.getPlay_long_1w.toDouble,
          y.getPlay_times_1w.toDouble,
          y.getCtr_2w.toDouble,
          y.getUv_ctr_2w.toDouble,
          y.getPlay_long_2w.toDouble,
          y.getPlay_times_2w.toDouble,
          y.getCtr_1m.toDouble,
          y.getUv_ctr_1m.toDouble,
          y.getPlay_long_1m.toDouble,
          y.getPlay_times_1m.toDouble,
          y.getMatchScore.toDouble,
          y.getPopScore.toDouble,
          y.getExampleAge.toDouble,
          y.getCate2Prefer.toDouble,
          y.getCatePrefer.toDouble,
          y.getAuthorPrefer.toDouble,
          y.getPosition,
          y.getTriggerNum.toDouble,
          y.getTriggerRank.toDouble,
          y.getHour,
          y.getPhoneBrand,
          y.getPhoneResolution,
          y.getTimeSinceLastWatchSqrt.toDouble,
          y.getTimeSinceLastWatch.toDouble,
          y.getTimeSinceLastWatchSquare.toDouble,
          mkArrayLong(parse2Array(y.getBehaviorCids), 10, "-1"),
          mkArrayString(parse2Array(y.getBehaviorC1ids), 10, "-1"),
          mkArrayLong(parse2Array(y.getBehaviorAids), 10, "-1"),
          mkArrayLong(parse2Array(y.getBehaviorVids), 10, "-1"),
          mkArrayString(parse2Array(y.getBehaviorTokens), 10, "-1"),
          y.getVideoId.toLong,
          y.getAuthorId.toLong,
          y.getCate1Id.toLong,
          y.getCateId.toLong
        )
      })
      import spark.implicits._
      modelRDD.repartition(1).toDF.createOrReplaceTempView("tempTable")
      spark.sql("show databases").show
      spark.sql("use headline_test").show
      //       spark.sql("drop table if exists headline_test.tmp_model_features").show
      /*       spark.sql("create table if not exists headline_test.tmp_model_features as " +
               "select * from tempTable")*/

      spark.sql("insert into table headline_test.tmp_model_features " +
        "select * from tempTable")
      println("成功插入数据到Hive！！")
    })


    ssc.checkpoint("hdfs://dev-node02:9000/spark/checkpoint/Streaming2Hive")
    ssc.start()
    ssc.awaitTermination()

  }

}
