package com.dgmall.sparktest.dgmallTest.app.Streaming.PV_UV

import java.lang

import com.alibaba.fastjson.JSON
import com.dgmall.sparktest.dgmallTest.app.Streaming.SQLContextSingleton
import com.dgmall.sparktest.dgmallTest.bean.{ClickTable, CommenRecord, ViewTable}
import com.dgmall.sparktest.dgmallTest.common.{HBaseUtils, ParseObject}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, HTable, Put}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * @Author: Cedaris
  * @Date: 2019/7/23 15:19
  */
object pv_uv_hbase {
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
    ssc.checkpoint("hdfs://psy831:9000//checkpoint")

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

    //计算view和click的PV
    val viewPv: DStream[Long] = viewDS
      .countByWindow(Seconds(batch * 6), Seconds(batch * 6))
    val clickPv: DStream[Long] = clickDS
      .countByWindow(Seconds(batch * 6), Seconds(batch * 6))

    //HBase 配置
    val tableName = "dgmall_headline_test"
    val quorum = "psy831"
    val port = "2181"
    val conf: Configuration = HBaseUtils.getHBaseConfiguration(quorum,port)
    conf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
    val jobConf = new JobConf()
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)

    //曝光的PV UV 至 HBase
    viewDS.foreachRDD(x => {
      val sc: SQLContext = SQLContextSingleton.getInstance(ssc.sparkContext)
      import sc.implicits._
      import sc.sql
      import sc.sql
      x.map(y => y).toDF().createOrReplaceTempView("view")
      val view_pu_df: DataFrame = sql(
        """
        select
        date_format(current_timestamp(),'yyyy-MM-dd HH:mm') as time,
        count(1) as pv,
        count(distinct user_id) as uv
        from view
        """.stripMargin)
      val view_pu_rdd: RDD[Row] = view_pu_df.select("time","pv","uv").rdd
      view_pu_rdd.foreach(println)
//      writeViewToHBase(view_pu_rdd)
      val admin: Admin = HBaseUtils.getHBaseAdmin(conf,tableName)
      val table: HTable = HBaseUtils.getTable(conf,tableName)

      view_pu_rdd.collect.map(x=> {
        val put = new Put(Bytes.toBytes(x.getString(0)))
        put.add(Bytes.toBytes("PV_UV"),Bytes.toBytes("view_pv"),
          Bytes.toBytes(x.getLong(1)))
        put.add(Bytes.toBytes("PV_UV"),Bytes.toBytes("view_uv"),
          Bytes.toBytes(x.getLong(2)))
        table.put(put)
        table.close()})
    })

    //点击的PV UV 至HBASE
    clickDS.foreachRDD(x => {
      val sc: SQLContext = SQLContextSingleton.getInstance(ssc.sparkContext)
      import sc.implicits._
      import sc.sql
      import sc.sql
      x.map(y => y).toDF().createOrReplaceTempView("click")
      val click_pu_df: DataFrame = sql(
        """
        select
        date_format(current_timestamp(),'yyyy-MM-dd HH:mm') as time,
        count(1) as pv,
        count(distinct user_id) as uv
        from click
        """.stripMargin)
      val click_pu_rdd: RDD[Row] = click_pu_df.select("time","pv","uv").rdd
      click_pu_rdd.foreach(println)
      //      writeViewToHBase(view_pu_rdd)
      val admin: Admin = HBaseUtils.getHBaseAdmin(conf,tableName)
      val table: HTable = HBaseUtils.getTable(conf,tableName)

      click_pu_rdd.collect.map(x=> {
        val put = new Put(Bytes.toBytes(x.getString(0)))
        put.add(Bytes.toBytes("PV_UV"),Bytes.toBytes("click_pv"),
          Bytes.toBytes(x.getLong(1)))
        put.add(Bytes.toBytes("PV_UV"),Bytes.toBytes("click_uv"),
          Bytes.toBytes(x.getLong(2)))
        table.put(put)
        table.close()})
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(false,true)

  }

  def writeViewToHBase(rdd:RDD[Row]): Unit ={
    //HBase 配置
    val tableName = "dgmall_headline_test"
    val quorum = "psy831"
    val port = "2181"
    val conf: Configuration = HBaseUtils.getHBaseConfiguration(quorum,port)
    conf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
    val jobConf = new JobConf()
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)

    val admin: Admin = HBaseUtils.getHBaseAdmin(conf,tableName)
    val table: HTable = HBaseUtils.getTable(conf,tableName)

    rdd.map(x=> {
      val put = new Put(Bytes.toBytes(x.getString(0)))
      put.add(Bytes.toBytes("PV_UV"),Bytes.toBytes("view_pv"), Bytes.toBytes(x.getInt(1)))
      put.add(Bytes.toBytes("PV_UV"),Bytes.toBytes("view_uv"), Bytes.toBytes(x.getInt(2)))
      table.put(put)
      table.close()
    })
  }
}
object SQLContextSingleton {
  @transient  private var instance: SQLContext = _
  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}














