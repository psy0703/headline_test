package com.dgmall.sparktest.demo

import java.sql.{DriverManager, Statement}

object FeatureEngineering {

  // mysql配置
  Class.forName("com.mysql.jdbc.Driver").newInstance()
  lazy val mysqlConnection = DriverManager.getConnection(
    "jdbc:mysql://192.168.11.164:3306/dg_recommend?characterEncoding=utf8",
    "root",
    "dg123456")
  val statement: Statement = mysqlConnection.createStatement()
  // hbase配置
  lazy val hbaseClient: HbaseClient = new HbaseClient()

  /**
    * 视频的热度分和发布时间写入hbase
    * @param statement
    * @param hbaseClient
    */
  // create 'match_item_features',{NAME=>'fea','VERSIONS'=>1}  -- hbase 建表
  def Feature2Hbase(statement: Statement,hbaseClient: HbaseClient) = {
    val tablename = "match_item_features"
    val famliyname = "fea"
    val colum = "f"
    val resultSet = statement.executeQuery(AnalysisSQL.GET_SCORE_AND_PUBLISHTIME)
    while (resultSet.next()) {
      val videoId = resultSet.getString("video_id")
      val issue_time = resultSet.getString("issue_time").split("\\.")(0)
      val score = resultSet.getDouble("score")
      val data = issue_time + "," + score
      println("data=" + data)
      hbaseClient.putdata(tablename, videoId, famliyname, colum, data)
    }
  }

  def main(args: Array[String]): Unit = {
       Feature2Hbase(statement,hbaseClient)
  }

}


