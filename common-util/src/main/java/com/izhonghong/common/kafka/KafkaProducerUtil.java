package com.izhonghong.common.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;


import java.util.Properties;

public  class KafkaProducerUtil {
    private static Properties props;
    static {
        props = new Properties();
        //kafka集群，broker-list
        props.put("bootstrap.servers", "10.248.161.20:9092,10.248.161.21:9092,10.248.161.22:9092");
        //props.put("bootstrap.servers", "192.168.2.113:9092,192.168.2.114:9092,192.168.2.116:9092");
        //props.put("bootstrap.servers", "192.168.2.231:9092,192.168.2.232:9092,192.168.2.233:9092");
        props.put("acks", "all");
        //重试次数
        props.put("retries", Integer.MAX_VALUE);
        //kafka幂等开启
        props.put("enable.idempotence",true);
        //批次大小
        props.put("batch.size", 163840);
        //等待时间
        props.put("linger.ms", 50);
        //RecordAccumulator缓冲区大小
        props.put("buffer.memory", 67108864);
        //单条消息最大大小
        props.put("max.request.size", 10485760);
        //失败重试间隔
        props.put("retry.backoff.ms", 500);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    public static Producer<String, String> getProducer(){
        return new KafkaProducer<>(props);
    }


}
