package com.dgmall.sparktest.addressWarehouse.bean

/**
  * @Author: Cedaris
  * @Date: 2019/7/22 10:28
  */
case class CnTable (
                     province_id:BigInt,
                     city_id:BigInt,
                     county_id:BigInt,
                     province_name:String,
                     city_name:String,
                     county_name:String,
                     cn_town_id:BigInt,
                     cn_town_name:String
                   ){

}
