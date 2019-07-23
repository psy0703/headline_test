package com.dgmall.sparktest.addressWarehouse

import java.util.Properties

import com.dgmall.sparktest.addressWarehouse.bean.{CnTable, ErpAndCnTable, ErpTable}
import org.apache.commons.logging.LogFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
  * @Author: Cedaris
  * @Date: 2019/7/19 17:57
  */
object Main {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf()
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
      .set("spark.debug.maxToStringFields", "100")
      .set("hive.exec.dynamic.parition", "true")
      .set("hive.exec.dynamic.parition.mode", "nonstrict")
      .set("spark.sql.warehouse.dir", "hdfs://psy831:9000/user/hive/warehouse")
      .set("javax.jdo.option.ConnectionURL", "jdbc:mysql://psy831:3306/hive?characterEncoding=UTF-8")
      .set("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
      .set("javax.jdo.option.ConnectionUserName", "root")
      .set("javax.jdo.option.ConnectionPassword", "root")
      .set("spark.driver.allowMultipleContexts", "true")
      .setAppName("OmallTest")
      .setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql
    /*
    tab_name
dwd_area_erp
dwd_city_cn
dwd_city_erp
dwd_county_cn
dwd_county_erp
dwd_province_cn
dwd_province_erp
dwd_town_cn
dwd_town_erp
dwd_village_cn
dws_area_cn
dws_area_cn_town
dws_area_erp
tmp_area
tmp_joinby_county
     */

    //    sql("show databases").show()
    sql("use dgmall")
    /*
    hive (dgmall)> desc tmp_joinby_county;
OK
col_name	data_type	comment
province_id         	string              	?id
city_id             	string              	?id
county_id           	string              	?id
province_name       	string              	???
city_name           	string              	???
county_name         	string              	???
cn_town_id          	string              	????id
cn_town_name        	string              	??????
erp_town_id         	string              	ERP ?id
erp_town_name       	string              	ERP ???
     */
    //读取ERP省市县镇信息
    val erp: DataFrame = sql(
      """
      select
      province_id,city_id,county_id,
      province_name,city_name,county_name,
      town_id as erp_town_id,
      town_name as erp_town_name,
      0 as cn_town_id,
      "0" as cn_town_name
      from dws_area_erp
      """.stripMargin)

    //读取国家统计局省市县镇信息
    val cnn: DataFrame = sql(
      """
      select id_pro as province_id,
      id_city as city_id,
      id_county as county_id,
      name_pro as province_name,
      name_city as city_name,
      name_county as county_name,
      0 as erp_town_id,
      "0" as erp_town_name,
      id_town as cn_town_id,
      name_town as cn_town_name
      from dws_area_cn_town
      """.stripMargin)

    val erpDS: Dataset[ErpAndCnTable] = erp.as[ErpAndCnTable]
    val cnDS: Dataset[ErpAndCnTable] = cnn.as[ErpAndCnTable]
    //    erpDS.unionByName(cnDS).show()
    val erp_cn: Dataset[ErpAndCnTable] = erpDS.union(cnDS)
    erp_cn.createOrReplaceTempView("cn_erp")

    //    println(erp_cn.filter(x => x.erp_town_id != 0).count())
    //    println(erp_cn.filter(x => x.cn_town_id != 0).count())

    //    erp_cn.write.csv("E:\\MyCode\\headline_test\\src\\main\\resources\\address")

    //将数据写入mysql
        val url = "jdbc:mysql://psy831:3306/test_area?characterEncoding=UTF-8"
        val table = "cn_town_nomatch" //student表可以不存在，但是url中指定的数据库要存在
        val driver = "com.mysql.jdbc.Driver"
        val user = "root"
        val password = "root"

        val prop = new Properties()
        prop.put("user",user)
        prop.put("password",password)

        /*erp_cn.write
          .mode(SaveMode.Append)
            .jdbc(url,table,prop)*/

    val cn1: Dataset[ErpAndCnTable] = erp_cn.where("cn_town_id != 0 and " +
      "cn_town_id is not null")
    val erp1: Dataset[ErpAndCnTable] = erp_cn.where("erp_town_id != 0 and " +
      "erp_town_id is not null")
    cn1.createOrReplaceTempView("cn_town")
    erp1.createOrReplaceTempView("erp_town")

    val r1: DataFrame = sql(
      """
      select
      t1.province_id,t1.city_id,t1.county_id,
      t1.province_name,t1.city_name,t1.county_name,
      t1.cn_town_id,t1.cn_town_name,
      t2.erp_town_id,t2.erp_town_name
      from cn_town t1
      left join erp_town t2
      on t1.county_id = t2.county_id
      where t1.province_id <> 65
      and substr(t1.cn_town_name,0,2) = substr(t2.erp_town_name,0,2)
      and t1.cn_town_id = t2.erp_town_id
      """.stripMargin)
    r1.createOrReplaceTempView("common")
    r1

    //过滤出erp中和统计局不一样的数据集，写入表erp_town_nomatch
    sql(
      """
        |select
        |a.province_id,a.city_id,a.county_id,
        |a.province_name,a.city_name,a.county_name,
        |a.town_id,a.town_name
        |from dws_area_erp a
        |left join common b
        |on a.town_id = b.erp_town_id
        |and a.town_name = b.erp_town_name
        |where b.province_id is null
      """.stripMargin)
//      .coalesce(1)
//      .write
//      .mode(SaveMode.Append)
//      .jdbc(url,table,prop)

    //过滤出统计局中与公共数据没有的数据集
    sql(
      """
      select
      id_pro,
      id_city,
      id_county,
      name_pro,
      name_city,
      name_county,
      id_town,
      name_town
      from dws_area_cn_town a
      left join common b
     on a.id_town = b.cn_town_id
     and a.name_town = b.cn_town_name
     where b.province_id is null
      """.stripMargin)
      /*.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .jdbc(url,table,prop)*/


    spark.stop()
  }


  def compareName(str1: String, str2: String): Boolean = {
    if (str1.substring(0, 2).equals(str2.substring(0, 2))) {
      true
    } else {
      false
    }
  }

}
