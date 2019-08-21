package com.dgmall.sparktest.tensortflow

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
 * @Author: Cedaris
 * @Date: 2019/8/21 11:47
 */
object TFRecordReader {
  private val LOG = LoggerFactory.getLogger("TFRecordReader")
  private val STOP_FLAG = "TEST_STOP_FLAG"
  def main(args: Array[String]): Unit = {
    //spark conf 配置
    val batch = 10
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("TFRecordReader")
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
      .set("spark.debug.maxToStringFields", "100")
      .setIfMissing("spark.master", "local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "20000")
      .set("spark.driver.memory", "1024M")

    //Spark streaming 配置
    val ssc = new StreamingContext(sparkConf, Seconds(batch))
    System.setProperty("HADOOP_USER_NAME", "dev")
//    ssc.checkpoint("hdfs://dev-node02:9000/spark/checkpoint/StreamingTest")


    val path = "hdfs://dev-node02:9000/spark/test-output-tfrecord/output.tfrecords"

    ssc.textFileStream(path)
      .foreachRDD(x=>{
        val spark = SparkSessionSingleton.getInstance(x.sparkContext.getConf)

          spark
            .read
            .format("tfrecords")
            .option("recordType", "Example")
            //.option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ") .
            .load(path)
            .show()

      })

    ssc.start()
    ssc.awaitTermination()

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
}
