package com.dgmall.sparktest;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * @Author: Cedaris
 * @Date: 2019/8/21 13:59
 */
public class KafkaTFRecord {
    private final String STOP_FLAG = "TEST_STOP_FLAG";
    Logger LOG = LoggerFactory.getLogger("KafkaTFRecord");

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("KafkaTFRecord2")
                .set("spark.streaming.kafka.consumer.cache.enabled", "false")
                .set("spark.debug.maxToStringFields", "100")
                .setIfMissing("spark.master", "local[*]")
                .set("spark.streaming.kafka.maxRatePerPartition", "20000")
                .set("spark.jars", "E:\\MyCode\\headline_test\\src\\main\\resources\\spark-tensorflow-connector_2" +
                        ".11-1.10.0.jar");

        System.setProperty("HADOOP_USER_NAME", "dev");

        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));
        ssc.checkpoint("hdfs://dev-node02:9000/spark/checkpoint/KafkaTFRecord");

        //kafka参数
        String bootstrapServers = "dev-node01:9092,dev-node02:9092,dev-node03:9092";
        String groupId = "kafka-tf";
        String topic = "modelFeatures";
        Collection<String> topics = Arrays.asList(topic);

        HashMap<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", bootstrapServers);
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

        kafkaStream.mapToPair(record -> new Tuple2(record.key(),record.value())).foreachRDD(new VoidFunction() {
            @Override
            public void call(Object o) throws Exception {


            }
        });

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
