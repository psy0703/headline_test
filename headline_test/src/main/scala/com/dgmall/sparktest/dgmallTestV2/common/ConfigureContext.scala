package com.dgmall.sparktest.dgmallTestV2.common

import java.util.ResourceBundle

/**
  * @Author: Cedaris
  * @Date: 2019/8/14 16:27
  */
object ConfigureContext {
  // 解析文件返回map
  def loadConfig(): Map[String, String] ={
    val bundle = ResourceBundle.getBundle("develop")
    var configMap:Map[String, String] = Map()
    val enum = bundle.getKeys
    while(enum.hasMoreElements){
      val key = enum.nextElement()
      configMap +=((key, bundle.getString(key)))
    }
    configMap
  }
}
