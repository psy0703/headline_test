package com.dgmall.sparktest.dgmallTest.app.Streaming

import com.alibaba.fastjson.JSON
import com.dgmall.sparktest.dgmallTest.appbean.{AppView, AppWatch}
import com.dgmall.sparktest.dgmallTest.bean.{CommenRecord, ViewTable, WatchTable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream

/**
  * 各个指标需求开发
  * UV（Unique visitor）
  * PV（Page View）
  *
  * @Author: Cedaris
  * @Date: 2019/7/18 16:25
  */
object IndexRequires {

  /**
    * 过滤出观看日志并进行相关操作
    *
    * @param inputstream
    * @param spark
    */
  def saveWatchData(inputstream: DStream[CommenRecord], spark: SparkSession): DStream[WatchTable] = {
    import spark.implicits._
    import spark.sql
    val watchDS: DStream[WatchTable] = inputstream
//      .window(Seconds(60), Seconds(20)) //每隔20秒钟，统计最近20秒的数据
      .filter(x => {if (x.Event.contains("play")) true else false}) //过滤观看日志
      .map(x => {
      val properties: String = x.Properties.toString
      val watch: AppWatch = JSON.parseObject(properties, classOf[AppWatch])
      //转换为观看表结构的类
      WatchTable(x.distinct_id, x.Time, x.Event, x.Type, watch.getTrace_id,
        watch.getAlg_match, watch.getAlg_rank, watch.getRule, watch
          .getBhv_amt, watch.getUser_id, watch.getVideo_id, watch
          .getVideo_user_id, watch.getVideo_desc, watch.getVideo_tag, watch
          .getWatch_time_long, watch.getVideo_long, watch.getMusic_name, watch
          .getMusic_write, watch.getVideo_topic, watch.getVideo_address, watch
          .getIs_attention, watch.getIs_like, watch.getIs_comment, watch
          .getIs_share_weixin, watch.getIs_share_friendster, watch
          .getIs_share_qq, watch.getIs_save, watch.getIs_get_red_packets,
        watch.getRed_packets_sum, watch.getIs_copy_site, watch.getIs_report,
        watch.getReport_content, watch.getIs_not_interested, watch
          .getIs_go_shop, watch.getShop_id, watch.getShop_name)
      /*//创建临时表
      val watchdf: DataFrame = watchRDD.toDF()
      watchdf.registerTempTable("watch")
      println(watchdf.count())*/
      /*//将数据写入Hive表中
      sql("use test")
//      sql("show tables").show()
      sql("insert into test.test_watch partition (log_time = " +
        "'2019-07-19') select * from watch")
      //      watchPvRealTime(spark)
      //统计观看pv
      val watchPV: DataFrame = sql("select count(*) from test.test_watch where log_time = '2019-07-18'")
      //TODO 将数据保存
      watchPV.show()

      //      watchUvRealTime(spark)
      //统计观看UV
      val watchUV: DataFrame = sql("select count(*) from test.test_watch where " +
        "log_time = '2019-07-19' group by user_id")
      //TODO 将数据保存
      watchUV.show()*/

    })
    watchDS
  }

  /**
    * 曝光浏览日志进行相关操作
    *
    * @param inputstream
    * @param spark
    */
  def saveViewData(inputstream: DStream[CommenRecord], spark: SparkSession)
  : Unit = {
    import spark.implicits._
    import spark.sql
    inputstream
      .window(Seconds(20), Seconds(20)) //每隔20秒钟，统计最近20秒的数据
      .filter(x => {
      if (x.Event.contains("view")) true else false
    }) //过滤浏览日志
      .foreachRDD(x => {

      val watchRDD: RDD[ViewTable] = x.map(y => {
        val properties: String = y.Properties.toString
        val view: AppView = JSON.parseObject(properties, classOf[AppView])
        //转换为观看表结构的类
        ViewTable(y.distinct_id, y.Time, y.Event, y.Type, view.getAlg_match,
          view.getAlg_rank, view.getRule, view.getUser_id, view.getVideo_id,
          view.getTrace_id)
      })
      //创建临时表
      watchRDD.toDF().createOrReplaceTempView("view")
      /*//将数据写入Hive表中
      sql("use test")
//      sql("show tables").show()
      sql("insert into test.test_view partition (log_time = " +
        "'2019-07-19') select * from view")

      //      viewPvRealTime(spark)
      //统计曝光浏览UV
      val viewUV: DataFrame = sql("select count(*) from test.test_view where " +
        "log_time = '2019-07-18' group by user_id")
      //TODO 将数据保存
      viewUV.show()

      //      viewUvRealTime(spark)
      //统计曝光浏览PV
      val viewPV: DataFrame = sql("select count(*) from test.test_watch where " +
        "log_time = '2019-07-19'")
      //TODO 将数据保存
      viewPV.show()*/
    })
  }

  def watchPv(spark: SparkSession): Unit = {

  }

  /**
    * 实时统计观看的PV
    *
    * @param spark
    */
  def watchPvRealTime(spark: SparkSession)
  : Unit = {
    import spark.implicits._
    import spark.sql
    val watchPV: DataFrame = sql("select count(*) from test.test_watch where log_time = '2019-07-18'")
    //TODO 将数据保存
    watchPV.show()

  }

  def watchUv(spark: SparkSession): Unit = {

  }

  /**
    * 实时统计观看的UV
    *
    * @param spark
    */
  def watchUvRealTime(spark: SparkSession): Unit = {
    import spark.implicits._
    import spark.sql

    //统计观看UV
    val watchUV: DataFrame = sql("select count(*) from test.test_watch where " +
      "log_time = '2019-07-18' group by user_id")
    //TODO 将数据保存
    watchUV.show()

  }

  /**
    * 离线统计曝光浏览的UV
    *
    * @param spark
    */
  def viewUv(spark: SparkSession): Unit = {

  }

  /**
    * 实时统计曝光浏览的UV
    *
    * @param spark
    */
  def viewUvRealTime(spark: SparkSession): Unit = {
    import spark.implicits._
    import spark.sql

    //统计曝光浏览UV
    val viewUV: DataFrame = sql("select count(*) from test.test_view where " +
      "log_time = '2019-07-18' group by user_id")
    //TODO 将数据保存
    viewUV.show()
  }

  /**
    * 离线统计曝光浏览的PV
    *
    * @param spark
    */
  def viewPv(spark: SparkSession): Unit = {

  }

  /**
    * 实时统计曝光浏览的Pv
    *
    * @param spark
    */
  def viewPvRealTime(spark: SparkSession): Unit = {
    import spark.implicits._
    import spark.sql

    //统计曝光浏览PV
    val viewPV: DataFrame = sql("select count(*) from test.test_watch where " +
      "log_time = '2019-07-18'")
    //TODO 将数据保存
    viewPV.show()
  }

}
