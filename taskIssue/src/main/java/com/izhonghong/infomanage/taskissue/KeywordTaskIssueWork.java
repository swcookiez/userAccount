package com.izhonghong.infomanage.taskissue;

import com.izhonghong.infomanage.taskissue.bean.KeyWordBean;
import com.izhonghong.infomanage.taskissue.util.KafkaProduce;
import com.izhonghong.infomanage.taskissue.util.ParseJsonData;
import com.izhonghong.infomanage.taskissue.util.QueryData;
import com.zhonghong.crawl.kafka.bean.KafkaMessage;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;


import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

//信息源管理平台关键词任务
public class KeywordTaskIssueWork extends TimerTask {
    private String sql ;
    private int param;
    private static final Logger LOG = Logger.getLogger(KeywordTaskIssue.class);
    public KeywordTaskIssueWork() {
    }

    public KeywordTaskIssueWork(String sql, int param) {
        this.sql = sql;
        this.param = param;
    }

    @Override
    public void run() {
        try{
            ArrayList<KeyWordBean> keyWordBeans = QueryData.KeyWordQuery(sql, param);
            if (!keyWordBeans.isEmpty()){
                Producer<String, String> producer = KafkaProduce.getProducer();
                List<KafkaMessage> kafkaMessages = new ArrayList<KafkaMessage>();
                for (KeyWordBean keyWordBean : keyWordBeans) {
                    String json = ParseJsonData.getJsonData(keyWordBean);
                    LOG.info("信息源管理平台："+json);
                    producer.send( new ProducerRecord<String,String>("taskIssue_keyword", json));
                }
                producer.close();
            }
        }catch (Exception e){
           e.printStackTrace();
        }
    }
}
