package com.dgmall.sparktest.dgmallTestV2.apps.recommader

import com.dgmall.sparktest.dgmallTestV2.caseclass.UserLevel
import com.dgmall.sparktest.dgmallTestV2.common.HBaseUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, HTable, Put}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author: Cedaris
  * @Date: 2019/8/14 13:47
  */
object Hive2HbaseTest {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf
    sc.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc.registerKryoClasses(Array(classOf[org.apache.hadoop.conf.Configuration]))

    val spark: SparkSession = SparkSession.builder()
      .config(sc)
      .appName("Hive2HbaseTest")
      .master("local[4]")
      .enableHiveSupport()
      .getOrCreate()

    System.setProperty("HADOOP_USER_NAME", "psy831")

    //HBase 配置
    val tableName = "headline_user_level"
    val cf = "user_level"
    val quorum = "psy831"
    val port = "2181"
    @transient val conf: Configuration = HBaseUtils.getHBaseConfiguration(quorum, port)
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val jobConf = new JobConf()
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)


    import spark.implicits._
    import spark.sql
    //    sql("show databases").show()
    sql("use headline_test")
    val user_levelDF: DataFrame = sql(
      """
      select
      user_id,
      sum_play_long,
      sum_play_times,
      play_long_rank,
      play_times_rank,
      value_type,
      frequence_type
      from headline_test.temp_user_level
      """.stripMargin)

    user_levelDF.show(10)

    val admin: Admin = HBaseUtils.getHBaseAdmin(conf, tableName)
    val table: HTable = HBaseUtils.getTable(conf, tableName)

    user_levelDF.as[UserLevel].collect().foreach(x => {
      val put = new Put(Bytes.toBytes(x.user_id))
      put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("sum_play_long"), Bytes
        .toBytes(x.sum_play_long))
      put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("sum_play_times"), Bytes
        .toBytes(x.sum_play_times))
      put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("play_long_rank"), Bytes
        .toBytes(x.play_long_rank))
      put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("play_times_rank"),
        Bytes.toBytes(x.play_times_rank))
      put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("value_type"), Bytes
        .toBytes(x.value_type))
      put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("frequence_type"), Bytes
        .toBytes(x.frequence_type))
      table.put(put)
    })
    table.close()
    admin.close()

    spark.close()
  }

}
