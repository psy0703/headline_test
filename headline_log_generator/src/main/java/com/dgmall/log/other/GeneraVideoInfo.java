package com.dgmall.log.other;

import com.alibaba.fastjson.JSONObject;
import com.dgmall.log.utils.CommonUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;

import static com.dgmall.log.utils.CommonUtils.SendMessage;
import static com.dgmall.log.utils.CommonUtils.generaRelease;

/**
 * @Author: Cedaris
 * @Date: 2019/8/22 17:46
 */
public class GeneraVideoInfo {

    private final static Logger logger = LoggerFactory.getLogger(GeneraVideoInfo.class);

    public static Random rand = new Random();

    public static void main(String[] args) {
        //kafka 配置
        Properties props = new Properties();
        //Kafka 服务器的主机名和端口号
        props.put("bootstrap.servers", "psy831:9092,psy832:9092,psy833:9092");
//        props.put("bootstrap.servers", "dev-node01:9092,dev-node02:9092,dev-node03:9092");
        // 等待所有副本节点的应答
        props.put("acks", "all");
        // 重试最大次数
        props.put("retries", 0);
        // 批消息处理大小
        props.put("batch.size", 16384);
        // 请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        CommonUtils cu = new CommonUtils();
        for (int i = 0; i < 10000000; i++) {
            JSONObject commonFields = cu.generaCommonFields();


            commonFields.put("Type", "release");
            commonFields.put("Event", "release");
            commonFields.put("Properties", generaRelease());
            SendMessage(logger, producer, commonFields, i);


            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
