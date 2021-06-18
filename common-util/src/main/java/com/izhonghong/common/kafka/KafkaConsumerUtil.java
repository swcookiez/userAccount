package com.izhonghong.common.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerUtil {
    static Properties props = new Properties();
    static {
        props.put("bootstrap.servers", "10.248.161.20:9092,10.248.161.21:9092,10.248.161.22:9092");
        //props.put("bootstrap.servers", "192.168.2.231:9092,192.168.2.232:9092,192.168.2.233:9092");
        //props.put("bootstrap.servers", "192.168.2.113:9092,192.168.2.114:9092,192.168.2.116:9092");
        props.put("max.poll.records","3000");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        //props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    public static KafkaConsumer<String, String> getConsumer(String topic, String groupId){
        props.put("group.id", groupId);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));//消费者订阅主题
        return consumer;
    }
}
