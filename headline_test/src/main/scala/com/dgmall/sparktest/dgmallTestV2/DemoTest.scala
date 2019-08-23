package com.dgmall.sparktest.dgmallTestV2

import com.dgmall.sparktest.dgmallTestV2.common.CommonUtils._
/**
  * @Author: Cedaris
  * @Date: 2019/8/19 11:07
  */
object DemoTest {

  def main(args: Array[String]): Unit = {

    val str = "WrappedArray(92, 嘉卸慢, 穷成蜡, 131, 绰履株, 茂拜挡)"
    val str2 = "WrappedArray(0.112, 0.199, 0.127, 0.163, 0.108)"
    val str3 = "WrappedArray(66856, 24468, 81335, 40904, 76804, 17475,1111,2222,3333,4444)"

    println(parse2Array(str).mkString)
    println(parse2Array(str2).mkString)
    println(parse2Array(str3).mkString)


    println(mkArrayString(parse2Array(str), 10, "-1").toList.toString())
    println(mkArrayDouble(parse2Array(str2), 10, "-1").toList.toString)
    println(mkArrayLong(parse2Array(str3), 10, "-1").mkString)

  }
}

