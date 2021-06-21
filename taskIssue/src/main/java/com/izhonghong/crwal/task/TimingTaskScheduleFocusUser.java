package com.izhonghong.crwal.task;

import com.izhonghong.common.hbase.HBaseGeneral;
import com.izhonghong.infomanage.taskissue.util.KafkaProduce;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.izhonghong.common.hbase.HBaseGeneral.getHBaseColValue;
import static com.izhonghong.crwal.task.user_type.CollectType.getSinaJson;

/**
 * 2021/3/29 14:52
 * 关注用户定时采集任务
 * @author sw
 * deploy 定时器没小时启动一次
 */
public class TimingTaskScheduleFocusUser {
    static Map<String,String> types = new HashMap();
    static {
        //设定采集账号类型以及rowKey
        types.put("1001","IsBigV");
        types.put("1002","isGovernment");
        types.put("1003","isMedia");
    }
    static SimpleDateFormat format = new SimpleDateFormat("HH");
    private static final Logger LOG = Logger.getLogger("crawl-task");

    public static void main(String[] args) {
        Table table = HBaseGeneral.getTable("crawl:task_period_control");
        for (String s : types.keySet()) {
            Get get = new Get(Bytes.toBytes(s));
            try {
                String format = TimingTaskScheduleFocusUser.format.format(new Date());
                if (format.substring(0,1).equals("0")) format = format.substring(1);
                Result result = table.get(get);
                String collect_times = HBaseGeneral.getHBaseColValue(result, "fn", "collect_times");
                String[] split = collect_times.split(",");
                List<String> strings = Arrays.asList(split);
                if (strings.contains(format)) taskSend(types.get(s)); // 下发任务
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void taskSend(String columnName){
        LOG.info(" "+columnName+"task begin");
        Producer<String, String> producer = KafkaProduce.getProducer();
        Table table = HBaseGeneral.getTable("crawl:focus_user");
        Scan scan = new Scan();
        scan = HBaseGeneral.addScanColumns(scan,"fn","uid",columnName);
        int i = 0;
        ResultScanner scanner = HBaseGeneral.getScanner(table, scan);
        for (Result result : scanner) {
            String colValue = getHBaseColValue(result,"fn",columnName);
            if (colValue.equals("1")){
                String uid = HBaseGeneral.getHBaseColValue(result, "fn", "uid");
                String uuid = String.valueOf(UUID.randomUUID());
                String task = getSinaJson(uid,uuid);
                producer.send(new ProducerRecord<>("weibo_focus_user",task));
                i++;
            }
        }
        LOG.info(" "+columnName+"发送成功数据量: "+i);
        scanner.close();
        producer.close();
        HBaseGeneral.tableClose(table);
    }
}