package common

import caseclass._
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import bean._

/**
  * @Author: Cedaris
  * @Date: 2019/7/17 16:57
  */
object ParseObject extends Serializable {

  /**
    * 将日志转化为包含distinct_id,time,event,type,Properties 的公共类
    *
    * @param item
    * @return
    */
  def parseJson(item: InputDStream[ConsumerRecord[String, String]])
  : DStream[CommenRecord] = {
    val ds: DStream[CommenRecord] = item.map(x => {
      val records: String = x.value()
      val json: CommenRecord = JSON.parseObject(records,
        classOf[CommenRecord])
      json
    })
    ds
  }


  def parseWatch(recordDs: DStream[CommenRecord]): DStream[WatchTable] = {
    recordDs
      .filter(x => {
        if (x.Event.contains("play")) true else false
      }) //过滤观看日志
      .map(x => {
        val properties: String = x.Properties.toString
        val watch: AppWatch = JSON.parseObject(properties, classOf[AppWatch])
        //      val words: Array[String] = watch.getTrace_id.split(".")
        //转换为观看表结构的类
        WatchTable(x.distinct_id, x.Time, x.Event, x.Type, watch.getTrace_id,
          watch.getOrder, watch.getUser_id, watch.getVideo_id,
          watch.getVideo_user_id, watch.getWatch_time_long,
          watch.getIs_attention, watch.getIs_like, watch.getIs_comment,
          watch.getIs_share_weixin, watch.getIs_share_friendster,
          watch.getIs_share_qq, watch.getIs_save, watch.getIs_get_red_packets,
          watch.getRed_packets_sum, watch.getIs_copy_site, watch.getIs_report,
          watch.getReport_content, watch.getIs_not_interested,
          watch.getIs_go_shop, watch.getShop_id, watch.getShop_name)
      })
  }


  def parseView(recordDs: DStream[CommenRecord]): DStream[ViewTable] = {
    recordDs
      .filter(x => {
        if (x.Event.contains("view")) true else false
      }) //过滤观看日志
      .map(x => {
        val properties: String = x.Properties.toString
        val view: AppView = JSON.parseObject(properties, classOf[AppView])
        //      val words: Array[String] = view.getTrace_id.split(".")
        //转换为曝光表结构的类
        ViewTable(x.distinct_id, x.Time, x.Event, x.Type,
          view.getUser_id, view.getVideo_id, view.getTrace_id
        )
      })
  }


  def parseClick(recordDs: DStream[CommenRecord]): DStream[ClickTable] = {
    recordDs
      .filter(x => {
        if (x.Event.contains("click")) true else false
      }) //过滤观看日志
      .map(x => {
        val properties: String = x.Properties.toString
        val click: AppClick = JSON.parseObject(properties, classOf[AppClick])
        //      val words: Array[String] = click.getTrace_id.split(".")
        //转换为点击表结构的类
        ClickTable(x.distinct_id, x.Time, x.Event, x.Type,
          click.getOrder,
          click.getTrace_id,
          click.getUser_id,
          click.getVideo_id
        )
      })
  }


  def parseUserActions(jsonStr: String): AppUserActions = {
    val uactions: AppUserActions = JSON.parseObject(jsonStr, classOf[AppUserActions])
    uactions
  }

  def parseVideoIndex(jsonStr: String): AppVideoIndex = {
    JSON.parseObject(jsonStr, classOf[AppVideoIndex])
  }

  def parseUserInfo(jsonStr: String): AppUserInfo = {
    JSON.parseObject(jsonStr, classOf[AppUserInfo])
  }

  def parseVideoInfo(jsonStr:String):AppReleaseVideo={
    JSON.parseObject(jsonStr,classOf[AppReleaseVideo])
  }

  /*def parseModelFeatures(userAction:AppUserActions,videoIndex:AppVideoIndex,
                         userInfo:AppUserInfo,videoInfo:AppReleaseVideo)
  :ModelFeatures={

    ModelFeatures()
  }*/

  /*      /*
        *  判断日志是否为观看日志
        * @param item 要解析的数据
        * @return 返回日志对象MyRecord
        */
      def IsWatchType(item:CommenRecord):Boolean={
        if("play".equals(item.Event)){
          true
        } else{
          false
        }
      }

    /**
      * 判断是否是曝光日志
      * @param item
      * @return
      */
    def IsViewType(item:CommenRecord):Boolean={
      if("view".equals(item.Event)){
        true
      } else{
        false
      }
    }

    /**
      * 判断是否是列表点击日志
      * @param item
      * @return
      */
    def IsClickType(item:CommenRecord):Boolean={
      if("click".equals(item.Event)){
        true
      } else{
        false
      }
    }

    /**
      * 判断是否是详情-浏览点击（行为）日志
      * @param item
      * @return
      */
    def IsBehaviorType(item:CommenRecord):Boolean={
      if("behavior".equals(item.Event)){
        true
      } else{
        false
      }
    }

    /**
      * 判断是否是搜索点击日志
      * @param item
      * @return
      */
    def IsSearchrType(item:CommenRecord):Boolean={
      if("search-click".equals(item.Event)){
        true
      } else{
        false
      }
    }

    /**
      * 判断是否是送礼日志
      * @param item
      * @return
      */
    def IsGiftType(item:CommenRecord):Boolean={
      if("gift".equals(item.Event)){
        true
      } else{
        false
      }
    }*/


  def main (args: Array[String]): Unit
  =
  {
    val str = "{\"Type\":\"watch_video\",\"distinct_id\":\"js+b32e9c47-0152-4961-a5be-555144556db9\",\"Event\":\"play\",\"time\":\"2019-07-17 17:28:29\",\"Properties\":{\"is_attention\":\"1\",\"alg_rank\":\"rank1\",\"rule\":\"rule0\",\"video_topic\":\"\",\"video_user_id\":\"59790\",\"video_long\":\"5797\",\"is_share_friendster\":\"1\",\"watch_time_long\":\"187\",\"video_address\":\"22LVX4G4\",\"is_like\":\"0\",\"is_comment\":\"0\",\"music_name\":\"荐晌\",\"is_report\":\"0\",\"video_desc\":\"倡\",\"is_not_interested\":\"1\",\"trace_id\":\"app1.scenes2.plan2.bucket2\",\"is_copy_site\":\"0\",\"is_get_red_packets\":\"0\",\"shop_name\":\"膝特产店\",\"is_share_weixin\":\"0\",\"is_save\":\"1\",\"is_share_qq\":\"0\",\"video_tag\":\"\",\"shop_id\":\"756976\",\"music_write\":\"裁\",\"user_id\":\"55646\",\"report_content\":\"...\",\"alg_match\":\"match1\",\"bhv_amt\":\"2\",\"video_id\":\"426123\"}}"
    val jSONObject: JSONObject = JSON.parseObject(str)
    val eventName: String = jSONObject.get("Event").toString
    if ("play".equals(eventName)) {
      println("true")
    } else {
      println("false")
    }
  }
}
