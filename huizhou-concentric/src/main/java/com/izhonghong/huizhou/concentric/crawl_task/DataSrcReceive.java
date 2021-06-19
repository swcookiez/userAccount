package com.izhonghong.huizhou.concentric.crawl_task;

import com.alibaba.fastjson.JSONObject;
import com.izhonghong.common.alijson.ParseJsonData;
import com.izhonghong.common.hbase.HBaseGeneral;
import com.izhonghong.common.kafka.KafkaConsumerUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;

/**
 * @author sw
 * @date 2021/1/6  10:08
 */
public class DataSrcReceive {
    private static final ArrayList<Put> putList = new ArrayList<Put>();
    public static void main(String[] args) {
        new Thread(() -> receive("huizhou_concentric_data_source","sw_huizhou")).start();
    }
    //采集任务数据源接收
    private static void receive(String topic,String groupId){
        KafkaConsumer<String, String> consumer = KafkaConsumerUtil.getConsumer(topic, groupId);
        Table table = HBaseGeneral.getTable("huizhou:concentric_task_source");
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(1000);
            writeHBase(records,table);
            consumer.commitAsync();
        }
    }

    private static void writeHBase(ConsumerRecords<String, String> records,Table table){
        for (ConsumerRecord<String, String> record : records) {
            JSONObject jsonData = ParseJsonData.getJsonData(record.value());
            if (jsonData!=null){
                String id = jsonData.containsKey("id")?jsonData.getString("id"): "";
                Put put = new Put(Bytes.toBytes(id));
                HBaseGeneral.jsonObject2Put(jsonData, "fn", put, "createTime", "deptId","deptName",
                        "id","isDel","mediaName","mediaType","crawlerUrl");
                putList.add(put);
            }
        }
        HBaseGeneral.tableDataWrite(table,putList);
    }
}
