package recommader

import com.alibaba.fastjson.JSONObject
import common.HBaseUtils._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, HBaseAdmin, Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.ListBuffer

/**
  * 存入HBASE 的value格式变为json
  * @Author: Cedaris
  * @Date: 2019/8/14 13:47
  */
object Hive2HbaseTest {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("Hive2HbaseTest2")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    System.setProperty("HADOOP_USER_NAME", "dev")



/*    // 用户行为汇总表
    val hiveTable = "headlineV2:app_user_actions_info_summary"
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

    val cf1 = "user_actions"
    loadHive2Hbase(spark,hiveTable,columnList,sqlQurry,cf1)

    println("成功将HIVE 中的app_user_actions_summary 导入 HBASE中")



    //视频指标汇总
    val table2 = "headlineV2:app_video_index_info_summary"
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
    val cf2 = "video_index"

    loadHive2Hbase(spark,table2,columnList2,sqlQurry2,cf2)

    println("成功将HIVE 中的app_video_summary导入到HBASE 中")


//  用户等级汇总(todo:有用户信息表后加入用户信息)
    val table3 = "headlineV2:app_user_actions_info_summary"
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
    var columnList3 = new ListBuffer[String]
    columnList3.append("user_id","sum_play_long","sum_play_times",
      "play_long_rank","play_times_rank","value_type","frequence_type")
    val cf3 = "User_Info"

    loadHive2Hbase(spark,table3,columnList3,sqlQurry3,cf3)

    println("成功将HIVE 中的user_info导入到HBASE 中")*/


    //视频信息
    val table4 = "headlineV2:app_video_info_summary"
    var columnList4 = new ListBuffer[String]
    //dwd_headline_video_info
    columnList4.append("user_id","video_id","upload_time","video_desc", "video_tag",
      "video_child_tag","video_long","music_name",
      "music_write","video_topic","video_address"
    )
    val sqlQurry4 =
      s"""
         |select *
         |from headline_test.dwd_headline_video_info
      """.stripMargin
    val cf4 = "video_info"

    loadHive2Hbase(spark,table4,columnList4,sqlQurry4,cf4)

    println("成功将HIVE 中的video_info导入到HBASE 中")


    spark.close()
  }

  /**
    * hbase获取指定列数据
    * @param tablename
    * @param rowkey
    * @param famliyname
    * @param colum
    * @return
    */
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
                     columnFamily:String): Unit ={

    //hbase新建表或者添加列族
    createTable(hiveTableName,columnFamily)

    import spark.sql

    sql("use headline_test")
    val sqlDF: DataFrame = sql(sqlQurry)

    // 写入数据到hbase
    sqlDF.foreachPartition(x => {
      //获取配置和表
      val conn = getHBaseConnection()
      val admin = conn.getAdmin.asInstanceOf[HBaseAdmin]
      val tableName = TableName.valueOf(hiveTableName)
      val table = conn.getTable(tableName)

      x.foreach(row => {
        //校验
        def checkValue(v: Any): String = if (v == null || v.toString.trim.eq("")) "null" else v.toString

        val rowkey = row.getString(0).getBytes()
        val cf = columnFamily.getBytes()
        val put = new Put(rowkey)

        //创建json对象
        val json = new JSONObject()
        for (i <- 0 until columnList.size) {
          //将K-V 加入json
          json.put(columnList(i),checkValue(row(i)))
        }
        //将json包装的数据写入hbase
        put.addColumn(cf,
          "info".getBytes(),
          json.toString().getBytes())
        table.put(put)
      })
      //关闭连接
      table.close()
      conn.close()
    })
  }

}
