package com.izhonghong.informationmanager.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.izhonghong.common.hbase.HBaseGeneral;
import com.izhonghong.common.kafka.KafkaProducerUtil;
import com.izhonghong.hbase.common.util.HbClient;
import com.izhonghong.hbase.common.util.Md5Utils;
import com.izhonghong.informationmanager.dao.ESQuery;
import com.izhonghong.informationmanager.dao.HBaseAPP;
import com.izhonghong.informationmanager.util.HttpUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.util.*;

/**
 *添加信息源写hbase
 *下发任务。
 */
@Service
public class UserServiceImp implements UserService{
    private String tableName = "infoManager:user_defined_account";
    private final byte[] colFamily = Bytes.toBytes("info");
    private final String URL = "http://121.36.38.216:8709/spider/source/information";


    //初始化sourceType,根据对应的type类型找到对应的表
    private static Map<String,String> sourceTypeAndIndex;
    static {
        sourceTypeAndIndex = new HashMap<>();
        sourceTypeAndIndex.put("2","wechat_biz_regionalization");
        sourceTypeAndIndex.put("3","weibo_user_regionalization");
        sourceTypeAndIndex.put("4","tt_user_regionalization");
    }

    @Override
    public ArrayList<String> getUsers(String tableName) {
        return null;
    }

    @Override
    public Boolean putUser(JSONArray paramss) {
        Boolean flag = false ;
        HTable hTable = HbClient.getHTable(tableName);
        ArrayList<Put> puts = new ArrayList<>();
        int size = paramss.size();
        for (int i = 0; i < size; i++) {
            JSONObject jsonObject = paramss.getJSONObject(i);
            String key = jsonObject.containsKey("key") ? jsonObject.getString("key") : "";
            String sourceType = jsonObject.containsKey("sourceType") ? jsonObject.getString("sourceType") : "";
            Put put = new Put(Bytes.toBytes(key));
            //自媒体转发逻辑
            sendMediaUserTask(jsonObject);
            for (String field : jsonObject.keySet()) {
                String value = jsonObject.getString(field);
                value = value!=null? value:"";
                put.addColumn(colFamily,Bytes.toBytes(field),Bytes.toBytes(value));
            }
            puts.add(put);
        }
        try {
            hTable.put(puts);
            flag = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return flag;
    }

    public void sendMediaUserTask(JSONObject json){
        Map<String,String> map = new HashMap<>();
        String sourceType = json.containsKey("sourceType") ? json.getString("sourceType") : "";
        if(sourceType.equals("2")){ //微信处理逻辑
            String wname = json.containsKey("wname") ? json.getString("wname") : "";
            String biz = json.containsKey("biz") ? json.getString("biz") : "";
            String account = json.containsKey("account") ? json.getString("account") : "";
            map.put("account_name",wname);map.put("app_id","300");
            map.put("account_id",biz);map.put("account",account);
        }else if(sourceType.equals("3")){ //微博处理
            String name = json.containsKey("name") ? json.getString("name") : "";
            String uid = json.containsKey("uid") ? json.getString("uid") : "";
            map.put("account_name",name);map.put("app_id","0");
            map.put("account_id",uid);
        }
        else { //其他自媒体处理
            String account_name = json.containsKey("account_name") ? json.getString("account_name") : "";
            String app_id = json.containsKey("app_id") ? json.getString("app_id") : "";
            String account_id = json.containsKey("account_id") ? json.getString("account_id") : "";
            String home_url = json.containsKey("home_url") ? json.getString("home_url") : "";
            map.put("account_name",account_name);map.put("app_id",app_id);
            map.put("account_id",account_id);map.put("url",home_url);
        }
        String result = HttpUtil.doPostForm(URL, map);
        System.out.println(result);
    }

    @Override
    public void deleteUser(JSONObject params) {}


    @Override
    public String userRegexQuery(String name,String sourceType,Long startRow,Long size,String province,String city,String mediaType){
        String json;
        String index = sourceTypeAndIndex.get(sourceType);
        try {
            json = ESQuery.userInfoQuery(startRow,size,index,name,province,city,mediaType);
        } catch (IOException e) {
            e.printStackTrace();
            json = "500";
        }
        return json;
    }

    //todo 使用定时器控制三种不同词的频率
    @Override
    public String addKeyword(JSONObject json) {
        String returnLog = "{\n" +
                "    \"errorCode\": \"9999\",\n" +
                "    \"errorMessage\": \"无法解析的异常信息\",\n" +
                "}";
        String type = json.getString("type");
        if ("1".equals(type)) {
            searchWordsDeal(json.getJSONArray("keywords")); //使用前端搜索词逻辑
            returnLog =  "{\n" +
                    "    \"errorCode\": \"0\",\n" +
                    "    \"errorMessage\": \"成功\",\n" +
                    "}";
        }
        else if("2".equals(type) || "3".equals(type)){
            subjectWordsDeal(json);
            returnLog =  "{\n" +
                    "    \"errorCode\": \"0\",\n" +
                    "    \"errorMessage\": \"成功\",\n" +
                    "}";
        }
        return returnLog;
    }


    //搜索词逻辑
    private static void searchWordsDeal(JSONArray keywords){
        if (keywords!=null){
            ArrayList<Put> puts = new ArrayList<>();
            Table table = HBaseGeneral.getTable("crawl:hotSearch");
            Producer<String, String> producer = KafkaProducerUtil.getProducer();
            for (Object o : keywords) {
                if(o instanceof String){
                    String keyword = (String) o;
                    JSONObject jsonTopic = new JSONObject();
                    jsonTopic.put("lexicon", keyword);
                    System.out.println(jsonTopic.toString());
                    producer.send( new ProducerRecord<String, String>( "taskIssue_keyword", jsonTopic.toJSONString()));
                    Map<String, String> row = new HashMap<>();
                    row.put("searchWord",keyword);
                    puts.add(HBaseAPP.getPut(keyword,row,7*3600*24*1000L));
                }
            }
            producer.close();
            HBaseGeneral.tableDataWrite(table,puts);
            HBaseGeneral.tableClose(table);
        }
    }

    //专题关键词或监测关键词处理逻辑
    private void subjectWordsDeal(JSONObject json) {
        Table table = HBaseGeneral.getTable("crawl:hotSearch");
        String subjectId = json.containsKey("subjectId") ? json.getString("subjectId"):"";
        JSONArray keywords = json.containsKey("keywords")? json.getJSONArray("keywords"):new JSONArray();
        Map<String, String> row = new HashMap<>();
        row.put("subjectId",subjectId);
        row.put("subjectWords",keywords.toString());
        Put put = HBaseAPP.getPut(subjectId, row, -1L);
        try {
            table.put(put);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getUserInfo(String tableName,String id) {
        Table table = HBaseGeneral.getTable(tableName);
        Get get = new Get(Bytes.toBytes(Md5Utils.getMd5ByStr(id)));
        JSONObject json = new JSONObject();
        try {
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                String field = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                json.put(field,value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            HBaseGeneral.tableClose(table);
        }
        return json.toString();
    }
}
