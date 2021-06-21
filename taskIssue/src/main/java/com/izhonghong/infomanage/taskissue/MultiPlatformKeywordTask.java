package com.izhonghong.infomanage.taskissue;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.izhonghong.common.hbase.HBaseGeneral;
import com.izhonghong.common.kafka.KafkaProducerUtil;
import com.izhonghong.infomanage.taskissue.util.ParseJsonData;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 2021/4/15 13:54
 *
 * @author sw
 * deploy
 * 多平台关键词工具
 */
public class MultiPlatformKeywordTask {

    //热搜词，调度任务 todo searchWord ,topic
    public static void HotSearchWord(String column){
        Producer<String, String> producer = KafkaProducerUtil.getProducer();
        Table table = HBaseGeneral.getTable("crawl:hotSearch");
        Scan scan = new Scan();
        scan = HBaseGeneral.addScanColumns(scan,"fn",column);
        ResultScanner scanner = HBaseGeneral.getScanner(table, scan);
        for (Result result : scanner) {
            String topic =  result.containsColumn(Bytes.toBytes("fn"), Bytes.toBytes(column))
                    ?Bytes.toString(result.getValue(Bytes.toBytes("fn"), Bytes.toBytes(column))):"";
            if (!"".equals(topic)){
                JSONObject jsonTopic = new JSONObject();
                jsonTopic.put("lexicon", topic);
                producer.send( new ProducerRecord<String, String>( "taskIssue_keyword", jsonTopic.toJSONString()));
            }
        }
        scanner.close();
        HBaseGeneral.tableClose(table);
        producer.close();
    }

    //专题词组subjectWords
    public static void subjectWords(String column){
        Producer<String, String> producer = KafkaProducerUtil.getProducer();
        Table table = HBaseGeneral.getTable("crawl:hotSearch");
        Scan scan = new Scan();
        scan = HBaseGeneral.addScanColumns(scan,"fn",column);
        ResultScanner scanner = HBaseGeneral.getScanner(table, scan);
        for (Result result : scanner) {
            String subjectWords =  result.containsColumn(Bytes.toBytes("fn"), Bytes.toBytes(column))
                    ?Bytes.toString(result.getValue(Bytes.toBytes("fn"), Bytes.toBytes(column))):"";
            if (!"".equals(subjectWords)){
                JSONArray jsonArray = ParseJsonData.getJsonArray(subjectWords);
                if(!jsonArray.isEmpty()){
                    for (Object o : jsonArray) {
                        String keyword = (String)o;
                        JSONObject jsonTopic = new JSONObject();
                        jsonTopic.put("lexicon", keyword);
                        producer.send( new ProducerRecord<String, String>( "taskIssue_subject_keyword", jsonTopic.toJSONString()));
                    }
                }
            }
        }
    }
}