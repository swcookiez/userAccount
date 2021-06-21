package com.izhonghong.infomanage.taskissue.writehbase;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.izhonghong.common.alijson.ParseJsonData;
import com.izhonghong.common.hbase.HBaseGeneral;
import com.izhonghong.common.kafka.KafkaConsumerUtil;
import com.izhonghong.hbase.common.util.Md5Utils;
import com.izhonghong.infomanage.taskissue.util.DateUtil;
import com.izhonghong.infomanage.taskissue.util.RedisUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import redis.clients.jedis.JedisCluster;

import java.util.*;

//删除redis set
/**
 * @deploy 10.248.161.31
 * 关键词日志 写入HBase
 */
public class KeywordWriteHBase {
    static String data = "";
    private static final String keywordLog = "infoManager:keywordLog";
    private static final String tt_user_task = "crawl:tt_user_task"; //tt用户下发表，需要存储name，uid。后续采集信息补充到tt用户表。
    private static final String short_video_user = "crawl:short_video_user"; //短视频用户表，用于存储短视频用户信息。
    private static final Table hTable = HBaseGeneral.getTable(keywordLog);
    private static final Table ttTable = HBaseGeneral.getTable(tt_user_task);
    private static final Table videoTable = HBaseGeneral.getTable(short_video_user);
    private static final JedisCluster jedis = RedisUtil.getJedisCluster();
    private static byte[] info = Bytes.toBytes("info");


    public static void main(String[] args) {
        keywordLog();
    }

    public static void keywordLog() {

        KafkaConsumer<String, String> consumer = KafkaConsumerUtil.getConsumer("taskIssue_return", "keywordLog_sw_2");
        ArrayList<Put> puts = new ArrayList<>(); //关键词putList
        ArrayList<Put> ttPuts = new ArrayList<>();
        ArrayList<Put> videoPuts = new ArrayList<>();
        Map<String, JSONObject> jsonMap = new HashMap<>();
        Map<String, JSONObject> jsonMapWeibo = new HashMap<>();
        Set<String> smembersSelfMedia = null;
        Set<String> smembersWeibo = null;

        while (true) {
            //获取最近3天微博的UID Set，如果没有的话就初始化
            smembersWeibo = getLastDaySet("_weiboKeywordLog");

            //获取今日日期
            String today = DateUtil.getLastNDay(0);

            //
            smembersSelfMedia = getLastDaySet("_selfMediaKeywordLog");

            ConsumerRecords<String, String> records = consumer.poll(500);

            for (ConsumerRecord<String, String> record : records) {
                data = record.value();
                //依据jason的类型不同去解析，微博传回数据为JSONArray，自媒体则头条等为JSON
                JSONObject jsonObject = ParseJsonData.getJsonData(data);
                JSONArray jsonArray = ParseJsonData.getJsonArray(data);
                if (jsonObject!=null){
                    String duplication_id = jsonObject.containsKey("duplication_id") ? jsonObject.getString("duplication_id") : "";
                    jsonMap.put(duplication_id,jsonObject);
                }else if (jsonArray!=null){
                    int size = jsonArray.size();
                    for (int i = 0; i < size; i++) {
                        JSONObject jsonWeibo = jsonArray.getJSONObject(i);
                        String mid = jsonWeibo.containsKey("mid") ? jsonWeibo.getString("mid") : "";
                        jsonMapWeibo.put(mid,jsonWeibo); //
                    }
                }
            }

            Set<String> keySetMedia = jsonMap.keySet();
            keySetMedia.removeAll(smembersSelfMedia); //移除最近三天有的UID。（去重）
            //处理去重后的自媒体关键词采集日志
            for (String duplication_id : keySetMedia) {
                JSONObject jsonObject = jsonMap.get(duplication_id);
                //                  自媒体关键词入库
                Put put = selfMediaKeywordLog(jsonObject, today+"_selfMediaKeywordLog", duplication_id);
                if (put!=null) puts.add(put);    //put 为null会报空指针
                //                  k3 id
                String k3_id = jsonObject.containsKey("k3_id") ? jsonObject.getString("k3_id") : "";
                switch (k3_id){
                    case "8595": //头条k3_id
                        Put ttPut = ttUser(jsonObject);
                        ttPuts.add(ttPut);
                        break;
                    case "20410": //b站 -- 只要是视频站执行一致逻辑
                    case "20382": //抖音
                        Put videoPut = videoUser(jsonObject);
                        videoPuts.add(videoPut);
                        break;
                    default:
                        break;
                }
            }

            //Set对比去重，主要程序时间消耗
            long start = System.currentTimeMillis();
            Set<String> keySetWeibo = jsonMapWeibo.keySet();
            keySetWeibo.removeAll(smembersWeibo);
            System.out.println(System.currentTimeMillis()-start);
            //处理去重后的微博关键词采集日志
            for (String mid : keySetWeibo) {
                JSONObject jsonObject = jsonMapWeibo.get(mid);
                // 微博处理
                Put put = weiboReceiveKeywordLog(jsonObject, today+"_weiboKeywordLog",mid);
                if (put!=null) puts.add(put);
            }

            jsonMap.clear();
            jsonMapWeibo.clear();
            smembersSelfMedia.clear();
            smembersWeibo.clear();
            HBaseGeneral.tableDataWrite(hTable, puts);
            HBaseGeneral.tableDataWrite(ttTable, ttPuts);
            HBaseGeneral.tableDataWrite(videoTable, videoPuts);
            consumer.commitAsync();  //手动异步提交offset
        }

    }

    public static Put videoUser(JSONObject jsonObject){
        String uid = jsonObject.containsKey("uid") ? jsonObject.getString("uid") : "";
        Put put = new Put(Bytes.toBytes(Md5Utils.getMd5ByStr(uid)));
        return HBaseGeneral.jsonObject2Put(jsonObject,"fn",put,"short_id","name","signature","uid","sec_uid","k3_id","address");
    }

    public static Put ttUser(JSONObject jsonObject){
        String uid = jsonObject.containsKey("uid") ? jsonObject.getString("uid") : "";
        Put put = new Put(Bytes.toBytes(Md5Utils.getMd5ByStr(uid)));
        return HBaseGeneral.jsonObject2Put(jsonObject,"fn",put,"uid","name","address");
    }

    public static Put selfMediaKeywordLog(JSONObject jsonObject,String setKey,String duplication_id){

            System.out.println("not_contain: "+duplication_id);
            Long crawlTime = jsonObject.containsKey("crawler_time") ? jsonObject.getLong("crawler_time") : 0L;
            String uid = jsonObject.containsKey("uid") ? jsonObject.getString("uid") : "";
            //todo 写入数据存duplication_id

            String userName = jsonObject.containsKey("name") ? jsonObject.getString("name") : "";
            String keyword = jsonObject.containsKey("keyword") ? jsonObject.getString("keyword") : "";
            String articleTitle = jsonObject.containsKey("title") ? jsonObject.getString("title") : "";
            String articleUrl = jsonObject.containsKey("url") ? jsonObject.getString("url") : "";
            Put put = new Put(Bytes.toBytes(getRowKey(keyword,duplication_id)),crawlTime);
            put.addColumn(info,Bytes.toBytes("userName"),Bytes.toBytes(userName));
            put.addColumn(info,Bytes.toBytes("uid"),Bytes.toBytes(uid));
            put.addColumn(info,Bytes.toBytes("keyword"),Bytes.toBytes(keyword));
            put.addColumn(info,Bytes.toBytes("articleTitle"),Bytes.toBytes(articleTitle));
            put.addColumn(info,Bytes.toBytes("crawlTime"),Bytes.toBytes(crawlTime));
            put.addColumn(info,Bytes.toBytes("articleUrl"),Bytes.toBytes(articleUrl));
            //写入redis
            jedis.sadd(setKey, duplication_id);
        return put;
    }

    public static Put weiboReceiveKeywordLog(JSONObject jsonObject,String setKey,String mid){

                String userName = jsonObject.containsKey("userName") ? jsonObject.getString("userName") : "";
                String uid = jsonObject.containsKey("uid") ? jsonObject.getString("uid") : "";
                String keyword = jsonObject.containsKey("keyword") ? jsonObject.getString("keyword") : "";
                String articleTitle = jsonObject.containsKey("articleTitle") ? jsonObject.getString("articleTitle") : "";
                Long crawlTime = jsonObject.containsKey("crawlTime") ? jsonObject.getLong("crawlTime") : 0L;
                String articleUrl = jsonObject.containsKey("articleUrl") ? jsonObject.getString("articleUrl") : "";
                    //todo 写入数据存mid
                Put   put = new Put(Bytes.toBytes(getRowKey(keyword,mid)),crawlTime);
                    put.add(info, Bytes.toBytes("userName"), Bytes.toBytes(userName));
                    put.add(info, Bytes.toBytes("uid"), Bytes.toBytes(uid));
                    put.add(info, Bytes.toBytes("keyword"), Bytes.toBytes(keyword));
                    put.add(info, Bytes.toBytes("articleTitle"), Bytes.toBytes(articleTitle));
                    put.add(info, Bytes.toBytes("crawlTime"), Bytes.toBytes(crawlTime));
                    put.add(info, Bytes.toBytes("articleUrl"), Bytes.toBytes(articleUrl));
                    //写入redis
                    jedis.sadd(setKey, mid);

        return put ;
    }
    //返回最近3天的文章uid Set，包括今天
    private static Set<String> getLastDaySet(String mediaType){
        Set<String> set = new HashSet<>();
        String [] daysKey = new String[3];
        for (int i = 0; i < 3; i++) {
            String day = DateUtil.getLastNDay(i);
            daysKey[i] = day+mediaType;
            if(!jedis.exists(daysKey[i])){
                jedis.sadd(daysKey[i],"init");
            }
        }
        for (String key : daysKey) {
            Set<String> smembers = jedis.smembers(key);
            set.addAll(smembers);
        }
        return set;
    }

    public static String getRowKey(String keyword,String mid){
        String keywordMd5 = Md5Utils.getMd5ByStr(keyword).substring(7, 23);
        return keywordMd5+mid;
    }
}