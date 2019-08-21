package com.dgmall.sparktest;


import lombok.Builder;

import java.io.Serializable;

/**
 * @Author: Cedaris
 * @Date: 2019/8/20 18:09
 */

@Builder
public class ModelFeatures2 implements Serializable {
    private String audience_id;
    private String item_id;
    private String click;
    private String city;
    private String value_type;
    private String frequence_type;
    private String cate1_prefer;
    private String cate2_prefer;
    private String weights_cate1_prefer;
    private String weights_cate2_prefer;
    private String cate2Id;
    private String ctr_1d;
    private String uv_ctr_1d;
    private String play_long_1d;
    private String play_times_1d;
    private String ctr_1w;
    private String uv_ctr_1w;
    private String play_long_1w;
    private String play_times_1w;
    private String ctr_2w;
    private String uv_ctr_2w;
    private String play_long_2w;
    private String play_times_2w;
    private String ctr_1m;
    private String uv_ctr_1m;
    private String play_long_1m;
    private String play_times_1m;
    private String matchScore;
    private String popScore;
    private String exampleAge;
    private String cate2Prefer;
    private String catePrefer;
    private String authorPrefer;
    private String position;
    private String triggerNum;
    private String triggerRank;
    private String hour;
    private String phoneBrand;
    private String phoneResolution;
    private String timeSinceLastWatchSqrt;
    private String timeSinceLastWatch;
    private String timeSinceLastWatchSquare;
    private String behaviorCids;
    private String behaviorC1ids;
    private String behaviorAids;
    private String behaviorVids;
    private String behaviorTokens;
    private String videoId;
    private String authorId;
    private String cate1Id;
    private String cateId;

}
