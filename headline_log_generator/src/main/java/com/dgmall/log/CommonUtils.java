package com.dgmall.log;

import java.util.ArrayList;
import java.util.Random;

/**
 * @Author: Cedaris
 * @Date: 2019/8/1 10:38
 */
public class CommonUtils {

    /**
     * 返回随机城市名
     * @return
     */
    public static String getRandomCity(){
        ArrayList<String>  citys= new ArrayList<>();
        citys.add("深圳");
        citys.add("北京");
        citys.add("上海");
        citys.add("广州");
        citys.add("长沙");
        citys.add("重庆");
        citys.add("厦门");
        citys.add("武汉");
        citys.add("杭州");
        citys.add("成都");

        Random random = new Random();
        int index = random.nextInt(10);
        String city = citys.get(index);
        return city;
    }

    /**
     * 返回指定范围的视频标签编码
     * @return
     */
    public static int getRandomLabel() {
        int max=80;
        int min=18;
        Random random = new Random();

        int s = random.nextInt(max)%(max-min+1) + min;
       return s;
    }

    /**
     * 返回指定范围的二级视频标签编码
     * @return
     */
    public static int getRandomClass2Label() {
        int max=180;
        int min=81;
        Random random = new Random();

        int s = random.nextInt(max)%(max-min+1) + min;
        return s;
    }

    public static void main(String[] args) {
        System.out.println(CommonUtils.getRandomCity());

    }

}
