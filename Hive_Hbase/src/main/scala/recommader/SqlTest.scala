package recommader

import org.apache.spark.sql.SparkSession

/**
  * @Author: Cedaris
  * @Date: 2019/8/12 9:14
  */
object SqlTest {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("SqlTest")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    //允许笛卡尔积
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    System.setProperty("HADOOP_USER_NAME", "dev")

    import spark.sql
    sql("show databases").show()
//    sql("use headline_test")

    val month = "2019-08"
    val day = "2019-08-07"
    val spe = "$."

    val mySql =
      """
        |show tables
        |""".stripMargin

    sql(mySql).show()
    println("操作成功！！！")

    spark.close()
  }
}
