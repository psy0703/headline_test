package recommader

import java.lang
import common.CommonUtils._
import com.alibaba.fastjson.JSON
import bean.AppModelFeatures
import caseclass.ModelFeatures
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  *
  * @Author: Cedaris
  * @Date: 2019/8/20 17:16
  */
object kafkaTfrecord {
  private val LOG = LoggerFactory.getLogger("kafkaTfrecord")
  private val STOP_FLAG = "TEST_STOP_FLAG"

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
      .setAppName("kafkaTfrecord")
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
      .set("spark.debug.maxToStringFields", "100")
      .setIfMissing("spark.master", "local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "20000")
      .set("spark.jars", "E:\\MyCode\\headline_test\\src\\main\\resources\\spark-tensorflow-connector_2.11-1.10.0.jar")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    System.setProperty("HADOOP_USER_NAME", "dev")

    val ssc = new StreamingContext(sc, Seconds(10))
    ssc.checkpoint("hdfs://dev-node02:9000/spark/checkpoint/kafkaTfrecord")

    //    val path  = "D:\\dgmall\\test\\test-output.tfrecord"
    val path = "hdfs://dev-node02:9000/spark/test-output-tfrecord/output.tfrecords"

    //kafka参数
    val bootstrapServers = "dev-node01:9092,dev-node02:9092,dev-node03:9092"
    val groupId = "kafka-kafka"
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

    modelDS.foreachRDD(x => {

      val spark = SparkSessionSingleton.getInstance(x.sparkContext.getConf)
      x.foreach(y => println(y))
      import spark.implicits._

        val modelRDD: RDD[ModelFeatures] = x.map(y => {

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

        val df: DataFrame = modelRDD.toDF()

        println("正在写入...")
//        df.write.format("tfrecords").mode("overwrite").option("recordType", "Example").save(path)

      spark.sql("show databases").show()

      df.write.mode("append").saveAsTable("tfrecord_data")

      spark.sql("show tables").show()
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(false, true)
  }


}

