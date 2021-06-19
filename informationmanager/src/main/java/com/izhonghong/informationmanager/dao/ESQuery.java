package com.izhonghong.informationmanager.dao;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.izhonghong.informationmanager.util.ESJest;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 2021/3/19 11:03
 *
 * @author sw
 * deploy
 */
public class ESQuery {
    static Map<String,String> types = new HashMap();
    static {
        //设定采集账号类型以及rowKey
        types.put("1001","IsBigV");
        types.put("1002","isGovernment");
        types.put("1003","isMedia");
    }

    public static String userInfoQuery(Long from,Long size,String index,String regexName,
                                String regexProvince,String regexCity,String mediaType) throws IOException {
        JSONArray jsonArray = new JSONArray();
        JestClient jest = ESJest.getClient();
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("{\n" +
                "  \"from\":"+from+", \n"+
                "  \"size\":"+size+", \n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": [\n");
        if (!regexName.equals("")) stringBuilder.append(addMatch("name",regexName)).append(",\n");
        if (!regexCity.equals("")) stringBuilder.append(addMatch("city",regexCity)).append(",\n");

        String[] split = mediaType.split(",");
        for (String s : split) {
            String s1 = types.get(s);
            if (s1!=null) stringBuilder.append(addMatch(s1,"1")).append(",\n");
        }

        if (regexProvince.equals("")){
            stringBuilder.deleteCharAt(stringBuilder.length() - 2);
        }else {
            stringBuilder.append(addMatch("province",regexProvince)).append("\n");
        }
        stringBuilder.append("      ]\n" +
                "    }\n" +
                "  }\n" +
                "}");

        if (regexName.equals("") &&  regexProvince.equals("") && regexCity.equals("") && mediaType.equals(""))
            stringBuilder = new StringBuilder("{\n}");

        Search search = new Search.Builder(stringBuilder.toString())
                .addIndex(index)
                .build();

        //执行操作
        SearchResult result = jest.execute(search);
        Long total = result.getTotal();

        List<? extends SearchResult.Hit<? extends HashMap, Void>> hits = result.getHits(new HashMap<String, Object>().getClass());
        for (SearchResult.Hit<? extends HashMap, Void> hit : hits) {
            JSONObject jsonObject = new JSONObject();
            HashMap source = hit.source;
            for (Object key : source.keySet()) {
                Object value = source.get(key);
                jsonObject.put((String)key,value);
            }
            jsonArray.add(jsonObject);
        }
        JSONObject totalJson = new JSONObject();
        totalJson.put("total",total);
        jsonArray.add(totalJson);
        return jsonArray.toString();
    }


    public static String addMatch(String field,String value){
        field = "\""+field+"\"";
        value = "\""+value+"\"";
        String tmp = "{" +
                "\"match\":{" +
                field +":"+value+
                "}" +
                "}";
        return tmp;
    }
}
