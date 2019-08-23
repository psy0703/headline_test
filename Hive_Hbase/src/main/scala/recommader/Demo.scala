package recommader

import caseclass.{ClickTable, ViewTable}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @Author: Cedaris
  * @Date: 2019/8/8 14:30
  */
object Demo {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Headline_demo")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    System.setProperty("HADOOP_USER_NAME", "psy831")
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    import spark.implicits._
    import spark.sql
    sql("show databases").show()
    sql("use headline_test")

    val viewDf: DataFrame = sql("select user_id,video_id," +
      "concat(user_id , '-', video_id) as u_vcode " +
      "from dwd_headline_view where day = " +
      "'2019-08-07'")
    val clickDf: DataFrame = sql("select user_id,video_id," +
      "concat(user_id , '-' , video_id) as u_vcode" +
      " from dwd_headline_click where day =" +
      " '2019-08-07'")

    viewDf.createOrReplaceTempView("view")
    clickDf.createOrReplaceTempView("click")

    val viewClickDf: DataFrame = sql(
      """
        select
        t1.user_id,
        t1.video_id,
        t2.user_id,
        t2.video_id,
        if(t2.video_id is null,0,1) as is_click
        from
        view t1
        left join click t2
        on t1.u_vcode = t2.u_vcode
      """.stripMargin)
    viewClickDf.createOrReplaceTempView("viewClick")

    sql("select * from viewClick ").show(30)



    spark.close()
  }

}
