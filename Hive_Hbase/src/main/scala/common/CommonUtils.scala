package common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Author: Cedaris
  * @Date: 2019/8/22 14:23
  */
object CommonUtils {
  //将字符串解析为字符串数组
  def parse2String(str: String): String = {
    //"WrappedArray(0, 0, 0, 0, 0, 0, 118, 0, 0, 0)"
    str.substring(13, str.length - 1)

  }

  //将字符串解析为字符串数组
  def parse2Array(str: String): Array[String] = {
    str.substring(13, str.length - 1).split(",")
  }

/*  def array2String(arr:Array[T]):String = {

  }*/


  /**
    * 返回指定长度的Array[String]
    *
    * @param arr
    * @param lenth     期待的新数组的长度
    * @param expectStr 长度不足时期待插入的元素
    * @return
    */
  def mkArrayString(arr: Array[String], lenth: Int, expectStr: String)
  : Array[String] = {
    var newArr = new Array[String](lenth)
    if ( arr.length >= lenth) {
      arr
    } else {
      newArr = arr
      for (i <- 0 to  (lenth - arr.length - 1)) {
        newArr = newArr :+ expectStr
      }
      newArr
    }
  }

  /**
    * 返回指定长度的Array[BigInt]
    *
    * @param arr
    * @param lenth     期待的新数组的长度
    * @param expectStr 长度不足时期待插入的元素
    * @return
    */
  def mkArrayLong(arr: Array[String], lenth: Int, expectStr: String): Array[Long] = {

    if (arr.length >= lenth) {
      var newArr = new Array[Long](0)
      for (ele <- arr) {
        newArr = newArr :+ ele.trim.toLong
      }
      return newArr
    } else {
      var newArr = new Array[Long](0)
      for (ele <- arr) {
        if (!ele.isEmpty) {
          newArr =  newArr :+ ele.trim.toLong
        } else {
          newArr :+ 0
        }
      }
      for (i <- 0 to lenth - arr.length - 1) {
        newArr =  newArr :+ expectStr.toLong
      }
      return newArr
    }

  }

  /**
    * 返回指定长度的Array[Double]
    *
    * @param arr
    * @param lenth     期待的新数组的长度
    * @param expectStr 长度不足时期待插入的元素
    * @return
    */
  def mkArrayDouble(arr: Array[String], lenth: Int, expectStr: String)
  : Array[Double] = {

    if (arr.length >= lenth) {
      var newArr = new Array[Double](lenth)
      for (ele <- arr) {
        newArr = newArr :+ ele.trim.toDouble
      }
      return newArr
    } else {
      var newArr = new Array[Double](0)
      for (ele <- arr) {
        if (!ele.isEmpty) {
          newArr = newArr :+ ele.trim.toDouble
        } else {
          newArr = newArr :+ 0.toDouble
        }
      }
      for (i <- 0 to lenth - arr.length - 1) {
        newArr = newArr :+ expectStr.toDouble
      }
      return newArr
    }
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
