package com.izhonghong.huizhou.concentric.crawl_task;

import com.izhonghong.common.hbase.HBaseGeneral;
import com.izhonghong.common.kafka.KafkaProducerUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author sw
 * @date 2021/1/6  9:53
 */
public class CrawlerTask {
    private static final Producer<String, String> producer = KafkaProducerUtil.getProducer();

    public static void main(String[] args) {
        new Thread(() -> taskSend("huizhou_concentric_crawl_task")).start(); //创建线程，发送采集任务。
    }

    //采集任务发送
    private static void taskSend(String topic){
        Table tasks = HBaseGeneral.getTable("huizhou:concentric_task_source");
        Scan scan = new Scan();
        ResultScanner scanner = HBaseGeneral.getScanner(tasks, scan);
        for (Result result : scanner) {
            if("false".equals(HBaseGeneral.getHBaseColValue(result,"fn","isDel"))){
                String json = HBaseGeneral.result2JsonStr(result, "fn", "crawlerUrl","deptId","deptName","mediaName","mediaType","id");
                producer.send(new ProducerRecord<String,String>(topic,json));
            }
        }
        scanner.close();
        HBaseGeneral.tableClose(tasks);
        producer.close();
    }
}