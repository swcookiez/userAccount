package com.izhonghong.infomanage.taskissue;

import com.izhonghong.hbase.common.util.HbClient;
import com.izhonghong.hbase.common.util.IsNum;
import com.izhonghong.hbase.common.util.ParseJsonData;
import com.izhonghong.infomanage.taskissue.bean.UserSourceBean;

import com.izhonghong.infomanage.taskissue.util.KafkaProduce;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.IOException;
import java.util.TimerTask;

public class InfosrcTaskIssueWork extends TimerTask {
    public static final byte[] info = Bytes.toBytes("info");
    public static final byte[] sources = Bytes.toBytes("sourceType");
    public static final byte[] name = Bytes.toBytes("name");
    public static final byte[] id = Bytes.toBytes("uid");
    public static final byte[] collectLevel = Bytes.toBytes("collectLevel");
    public static final byte[] home_url = Bytes.toBytes("home_url");
    public static final byte[] biz = Bytes.toBytes("biz");
    private String params;


    @Override
    public void run() {
        HTable hTable  = null;
        Producer<String, String> producer =null;
        try {
            hTable = HbClient.getHTable("infoManager:user_defined_account");
            producer = KafkaProduce.getProducer();
            ResultScanner scanner = HbClient.getScanner(hTable);
            for (Result result : scanner) {
                String src = result.containsColumn(info, sources)?Bytes.toString(result.getValue(info, sources)) :"";
                //任务下发微博
                if (src.equals("3")){
                    String nam = result.containsColumn(info, name)?Bytes.toString(result.getValue(info, name)):null;
                    String ids = result.containsColumn(info, id)?Bytes.toString(result.getValue(info, id)):null;
                    String colv = result.containsColumn(info, collectLevel)?Bytes.toString(result.getValue(info, collectLevel)):"-1";
                    int colvInt = IsNum.isNumeric(colv)?Integer.parseInt(colv):-1;
                    String jsonData = ParseJsonData.getJsonData(new UserSourceBean(ids,nam,colvInt));
                    //发送指定采集等级的数据到topic
                    if (colv.equals(params)){
                        System.out.println(jsonData);
                        producer.send(new ProducerRecord<String,String>("taskIssue_infosrc", jsonData));
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (producer!=null) producer.close();
            if(hTable!=null) HbClient.closeHTable(hTable);
        }
    }


    public InfosrcTaskIssueWork(String params) {
        this.params = params;
    }
}
