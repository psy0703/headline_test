package com.dgmall.sparktest.addressWarehouse.bean

/**
  * @Author: Cedaris
  * @Date: 2019/7/22 10:29
  */
case class ErpAndCnTable (
                           province_id:BigInt,
                           city_id:BigInt,
                           county_id:BigInt,
                           province_name:String,
                           city_name:String,
                           county_name:String,
                           erp_town_id:BigInt,
                           erp_town_name:String,
                           cn_town_id:BigInt,
                           cn_town_name:String
                         ){

}
