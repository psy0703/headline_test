package com.dgmall.sparktest.demo

/**
  * spark分析计算sql类
  */
object AnalysisSQL {


    //用户行为信息提取sql 用于Item-based CF
    val USER_ITEM_SCORE =
      """
        |
        |select cast(user_id as string)
        |   ,cast(video_id as string)
        |	  ,cast(video_play_percent as double)
        |	  from dgmall_headline.dw_user_actions
        |where video_play_percent is not null
        |and user_id <> 0
        |and video_play_percent >0
        |
      """.stripMargin

  //用户行为信息提取sql 用于User-based CF
  val ITEM_USER_SCORE =
    """
      |
      |select cast(video_id as string)
      |   ,cast(user_id as string)
      |	  ,cast(video_play_percent as double)
      |	  from dgmall_headline.dw_user_actions
      |where video_play_percent is not null
      |and user_id <> 0
      |and video_play_percent >0
      |
      """.stripMargin



  // 视频热度分
  val ITEM_HOT_SCORE =
    """
      |
      |select video_id
      |      ,score
      |from dgmall_headline.dw_video_score
      |order by score desc limit 100
      |
      """.stripMargin

  // 视频内容信息提取用于tf-idf
  val ITEM_CONTENT_TFIDF =
    """
      |select video_id
      |,regexp_replace(video_desc, '\\s+', '')
      |,video_tag
      |,music_name
      |,music_write
      |from dgmall_headline.dw_video_info
    """.stripMargin

  // 视频内容信息提取用于word2vec
  val ITEM_CONTENT_W2V =
    """
      |select video_id
      |,video_desc
      |from dgmall_headline.dw_video_info
      |-- limit 200
    """.stripMargin

  val W2V_LSH_MATCH =
    """
      |  select video_id
      |  ,concat_ws(',',collect_set(id_sim)) as sim
      |  from w2v_lsh_match
      |  where video_id != id_sim
      |  group by video_id
    """.stripMargin


  // 计算视频特征
  val VIDEO_INFO =
  """
    |select t.video_id
    |		,province
    |		,city
    |		,area
    |		,town
    |		,video_desc as title
    |		,music_name
    |		,music_write as music_author
    |		,user_id as video_author
    |		--,regexp_replace(video_tag,"，","|") as tag
    |   ,video_tag
    |		,datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),substr(issue_time,1,10)) as example_age
    |		,t2.score as hot_score
    |from dgmall_headline.dw_video_info t
    |left join dgmall_headline.dw_video_score t2 on t.video_id = t2.video_id
  """.stripMargin

  val VIDEO_FEATURE =
    """
      |select t.video_id
      |		,t.province
      |		,t.city
      |		,t.area
      |		,t.town
      |   ,t.title
	    |   ,t.keywords
      |   ,t.music_name
	    |   ,t.music_author
	    |   ,t.video_author
	    |   ,t.tag
	    |   ,t.example_age
      |	  ,t.hot_score
      from dgmall_headline.dw_video_feature t
    """.stripMargin


  // 计算用户特征
  val USER_ACTION_INFO =
    """
      |select t.user_id
      |		,t3.province
      |		,t3.city
      |		,t3.area
      |		,t3.town
      |		,keywords
      |		,tag
      |		,music_name
      |		,music_author
      |		,video_author
      |		,video_play_percent
      |
      |from dgmall_headline.dw_user_actions_d t
      |left join dgmall_headline.dw_video_feature t2 on t.video_id = t2.video_id
      |left join dgmall_headline.dw_user_info t3 on t.user_id = t3.user_id
      |where
      |-- day between date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),29) and from_unixtime(unix_timestamp(),'yyyy-MM-dd') and
      |video_play_percent is not null
      |and video_play_percent>0
      |and t.user_id<>'0'
    """.stripMargin

  val USER_FEATURE =
    """
      |select t.user_id
      |		,t.province
      |		,t.city
      |		,t.area
      |		,t.town
      |   ,t.keywordLstStr
      |   ,t.tagLstStr
      |from dgmall_headline.dw_user_feature t
    """.stripMargin

   // 正负样本
  val USER_VIDEO_LABEL =
     """
       |	select
       |	     user_id
       |		  ,video_id
       |		  ,label
       |	from dgmall_headline.dw_user_video_label
     """.stripMargin

  // 计划信息
val RECOMMEND_PLAN_INFO =
  """
    |select
    |plan_id
    |,plan_name
    |,plan_version_id
    |,plan_start_time
    |,plan_end_time
    |,plan_status
    |,pos_code
    |,pos_name
    |,strategy_layer
    |,strategy_id
    |,strategy_name
    |,strategy_rule
    |,flow_proportion
    |from pa_plan_strategies
    |where plan_status = '10'
    |and date_format(CURDATE(),'%Y-%m-%d') between date_format(plan_start_time,'%Y-%m-%d') and date_format(plan_end_time,'%Y-%m-%d')
    |
  """.stripMargin

  // 计算推荐策略信息
  val RECOMMEND_STRATEGY_INFO =
    """
      |select id
      |,function
      |,strategy_name
      |,strategy_layer
      |from al_strategies
    """.stripMargin

  // 计算编辑推荐结果
  val EDITOR_REC_LIST =
    """
      |select video_id
      |from tb_headline_video_score
    """.stripMargin

  // 获取召回前缀
  val GET_RECALL_TYPE =
    """
      |select id
      |from al_strategies
      |where status=10 and strategy_name=
    """.stripMargin

  // 获取视频的发布时间和热度分
  val GET_SCORE_AND_PUBLISHTIME =
    """
      |select video_id
      |,issue_time
      |,score
      |from tb_headline_video_score
    """.stripMargin

  // 获取用户最近观看的视频列表 topN
  val VIDEO_ORDER_BY_USER=
    """
      |drop table if exists dgmall_headline.dw_user_actions_tmp
    """.stripMargin
  val VIDEO_ORDER_BY_USER2 =
    """
      |  create table  dgmall_headline.dw_user_actions_tmp as
      |  select user_id
      |  ,video_id
      |  ,count_time
      |  ,row_number() over(partition by user_id order by count_time desc) rn
      |   from dgmall_headline.dw_user_actions t
      |   where user_id <>0
    """.stripMargin


  val GET_VIDEOLIST_BY_USER =
    """
      |  select user_id
      |  ,concat_ws(',',collect_set(cast(video_id as string))) as video_ids
      |  from dgmall_headline.dw_user_actions_tmp t
      |  where rn <=10
      |  group by user_id
    """.stripMargin



}
