package com.dgmall.sparktest.dgmallTestV2.apps.recommader

import com.dgmall.sparktest.dgmallTestV2.bean.Constants
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * 将hive中的指定表数据导入到hbase中
  * @Author: Cedaris
  * @Date: 2019/8/14 16:17
  */
object Hive2Hbase {
  def main(args: Array[String]): Unit = {

    //todo 将创建表给为添加列族 alter 'tablename', 'columnfamily2'



    val spark: SparkSession = SparkSession.builder()
      .appName("Hive2Hbase")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    System.setProperty("HADOOP_USER_NAME", "dev")

    // hbase表
    val hiveTable = "headline:app_user_actions_summary"
    //Hive 表中的列
    //app_user_actions_summary
    var columnList = new ListBuffer[String]
    columnList.append("user_id", "time", "watch_last_time", "timesincelastwatch",
      "timesincelastwatchsqrt", "timesincelastwatchsquare", "behaviorvids",
      "behavioraids", "behaviorcids", "behaviorc1ids", "behaviortokens",
      "cate1_prefer", "weights_cate1_prefer", "cate2_prefer", "weights_cate2_prefer")

    // 写入数据到hbase
    val sqlQurry =
      """
        |select *
        |from headline_test.app_user_actions_summary
      """.stripMargin

    val cf1 = Constants.HBASE_COLUMN_FAMILY
    loadHive2Hbase(spark,hiveTable,columnList,sqlQurry,cf1)

  println("成功将HIVE 中的app_user_actions_summary 导入 HBASE中")

    val table2 = "headline:app_video_summary"
    var columnList2 = new ListBuffer[String]
    //app_video_summary
    columnList2.append("video_id",
      "ctr_1day","uv_ctr_1day","play_long_1day", "play_times_1day",
      "ctr_1week","uv_ctr_1week","play_long_1week", "play_times_1week",
      "ctr_2week","uv_ctr_2week","play_long_2week", "play_times_2week",
      "ctr_1month","uv_ctr_1month","play_long_1month","play_times_1month"
    )
    val day = "2019-08-14"
    val sqlQurry2 =
      s"""
        |select *
        |from headline_test.app_video_summary
        |where day = '${day}'
      """.stripMargin
    val cf2 = day

    loadHive2Hbase(spark,table2,columnList2,sqlQurry2,cf2)

    println("成功将HIVE 中的app_video_summary导入到HBASE 中")

    val table3 = "headline:user_level"
    val sqlQurry3 =
      """
        |select
        |      user_id,
        |      sum_play_long,
        |      sum_play_times,
        |      play_long_rank,
        |      play_times_rank,
        |      value_type,
        |      frequence_type
        |from headline_test.temp_user_level
      """.stripMargin
    var userLevelList = new ListBuffer[String]
    userLevelList.append("user_id","sum_play_long","sum_play_times",
    "play_long_rank","play_times_rank","value_type","frequence_type")
    val cf3 = "User_Level"

    loadHive2Hbase(spark,table3,userLevelList,sqlQurry3,cf3)

    println("成功将HIVE 中的user_level导入到HBASE 中")

    spark.close()
  }

  /**
    * 获取HBASE 连接
    * @return
    */
  def getHBaseConnection() = {
    val conf = HBaseConfiguration.create
    /*conf.set("hbase.zookeeper.property.clientPort", Constants.ZOOKEEPER_CLIENT_PORT)
    conf.set("hbase.zookeeper.quorum", Constants.ZOOKEEPER_QUORUM)
    conf.set("hbase.master", Constants.HBASE_MASTER)
    conf.set("zookeeper.znode.parent", Constants.ZOOKEEPER_ZNODE_PARENT)*/
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "192.168.11.165")
    conf.set("hbase.master", "192.168.11.164")
    conf.set("zookeeper.znode.parent", "/hbase")
    ConnectionFactory.createConnection(conf)
  }

  /**
    * HBASE中表不存在就创建表
    * @param hiveTable
    * @param columnFamily
    */
  def createTable(hiveTable: String,columnFamily:String): Unit = {
    val conn = getHBaseConnection()
    val admin = conn.getAdmin.asInstanceOf[HBaseAdmin]
    val tableName = TableName.valueOf(hiveTable)
    if (!admin.tableExists(tableName)) {
      // 表不存在则创建
      val desc = new HTableDescriptor(tableName)
      val columnDesc = new HColumnDescriptor(columnFamily)
      desc.addFamily(columnDesc)
      admin.createTable(desc)
    }else{
      val tableDesc: HTableDescriptor = admin.getTableDescriptor(tableName)
      admin.disableTable(tableName)
      //若列族已存在，先删除
      tableDesc.removeFamily(Bytes.toBytes(columnFamily))
      //创建新的列族
      val columnDescriptor = new HColumnDescriptor(columnFamily)
      tableDesc.addFamily(columnDescriptor)
      admin.modifyTable(tableName,tableDesc)
      admin.enableTable(tableName)
    }
  }

  def getData(tablename: String, rowkey: String, famliyname: String,
              colum: String): String = {
    val conn = getHBaseConnection()
    val table = conn.getTable(TableName.valueOf(tablename))
    // 将字符串转换成byte[]
    val rowkeybyte: Array[Byte] = Bytes.toBytes(rowkey)
    val get = new Get(rowkeybyte)
    val result: Result = table.get(get)
    val resultbytes = result.getValue(famliyname.getBytes, colum.getBytes)
    if (resultbytes == null) {return null}
    new String(resultbytes)
  }

  /**
    * 将hive中的数据导入hbase中
    * @param spark spark session
    * @param hiveTableName 要创建的hbase表名
    * @param columnList Hive 表中的列
    * @param sqlQurry 执行的sql语句
    */
  def loadHive2Hbase(spark: SparkSession,hiveTableName:String,
                     columnList:ListBuffer[String],sqlQurry:String,
                     columnFamily:String)
  : Unit ={
    //hbase新建表或者添加列族
    createTable(hiveTableName,columnFamily)

    import spark.sql
    //查询hive 数据
    sql("use headline_test")
    val sqlDF: DataFrame = sql(sqlQurry)
    sqlDF.show(10)

    // 写入数据到hbase
    sqlDF.foreachPartition(x => {
      val conn = getHBaseConnection()
      val admin = conn.getAdmin.asInstanceOf[HBaseAdmin]
      val tableName = TableName.valueOf(hiveTableName)
      val table = conn.getTable(tableName)

      x.foreach(row => {
        def checkValue(v: Any): String = if (v == null || v.toString.trim.eq("")) "null" else v.toString

        val rowkey = row.getString(0).getBytes()
        val cf = columnFamily.getBytes()
        val put = new Put(rowkey)

        for (i <- 0 until columnList.size) {
          put.addColumn(cf,
            columnList(i).getBytes(),
            checkValue(row(i)).getBytes())
        }
        table.put(put)
      })
      table.close()
      conn.close()
    })
  }
}
