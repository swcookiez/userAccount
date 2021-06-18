package com.izhonghong.common.alijson;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class ParseJsonData {

    public static JSONObject getJsonData(String data) {
        try {
            return JSONObject.parseObject(data);
        } catch (Exception e) {
            return null;
        }
    }

    public static String getJsonData(Object obj){
        try {
            return JSONObject.toJSONString(obj);
        }catch (Exception e){
            return null;
        }
    }

    public static JSONObject getJson(Object obj){
        try {
            String jsonString = getJsonData(obj);
           return  getJsonData(jsonString);
        }catch (Exception e){
            return null;
        }
    }

    public static JSONArray getJsonArray(String text){
        try {
            JSONArray jsonArray = JSONArray.parseArray(text);
            return jsonArray;
        }catch (Exception e){
            return null ;
        }
    }
}