package com.izhonghong.informationmanager.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.izhonghong.common.hbase.HBaseGeneral;
import com.izhonghong.hbase.common.util.IsNum;
import com.izhonghong.hbase.common.util.Md5Utils;
import com.izhonghong.hbase.common.util.ParseJsonData;
import com.izhonghong.informationmanager.bean.*;
import com.izhonghong.informationmanager.service.UserService;
import com.izhonghong.informationmanager.util.HBaseTableGet;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.io.IOException;
import java.util.ArrayList;

/**
 * @deploy 10.248.161.7
 * 部署命令 bash /home/zhonghong/infomanage/infosourceProject/infomanager.sh start
 */
@RestController
public class UsersController {
    @Autowired
    UserService userService;

    //请求添加或修改信息源信息
    @RequestMapping(value = "/api/v1/infosource/classify/users",method = RequestMethod.POST)
    public String addUsers(@RequestBody JSONArray params){
        Boolean flag = userService.putUser(params);
        if(flag){
            return "{\n" +
                    "    \"errorCode\": \"0\",\n" +
                    "    \"errorMessage\": \"成功\",\n" +
                    "}";
        }else {
            return "{\n" +
                    "    \"errorCode\": \"9999\",\n" +
                    "    \"errorMessage\": \"无法解析的异常信息\",\n" +
                    "}";
        }
    }

    //信息源采集统计
    @GetMapping("/crawl/statistics")
    public String crawlStatistics(@RequestParam(value="infoName",required = false)String infoName,
                                  @RequestParam(value = "infoSourceName",required = false)String infoSourceName,
                                  @RequestParam(value = "type",required = false,defaultValue = "0")Integer type,
                                  @RequestParam(value = "infoState",required = false,defaultValue = "1")Integer infoState,
                                  @RequestParam(value = "collectLevel",required = false)Integer collectLevel,
                                  @RequestParam(value = "page",required = false,defaultValue="1")int page,
                                  @RequestParam(value = "pageSize",required = false,defaultValue = "20")int pageSize){
        String informationStatsTableName = "infoManager:infoSourceCountResult";
        HTable informationStatsTable = HBaseTableGet.getInfoSourceCountResult(informationStatsTableName);
        JSONArray jsonArray = new JSONArray();
         byte[] infos = Bytes.toBytes("info");
         byte[] infoNames = Bytes.toBytes("infoName");
         byte[] infoSourceNames = Bytes.toBytes("infoSourceName");
         byte[] counts = Bytes.toBytes("count");
         byte[] dayCounts = Bytes.toBytes("dayCount");
         byte[] infoStates = Bytes.toBytes("infoState");
         byte[] collectLevels = Bytes.toBytes("collectLevel");
         byte[] types = Bytes.toBytes("type");

        //开始行
        Integer startRow = (page -1) * pageSize;
        Integer count = 0;  //计数
        Scan scan = new Scan();

        FilterList filterList  = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        //获取媒体类型
        if (type == 1){
            SubstringComparator substringComparator = new SubstringComparator("1");
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"),
                    Bytes.toBytes("type"), CompareFilter.CompareOp.EQUAL,substringComparator);
            filterList.addFilter(filter);
        }else if(type ==2 ){
            SubstringComparator substringComparator = new SubstringComparator("2");
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"),
                    Bytes.toBytes("type"), CompareFilter.CompareOp.EQUAL,substringComparator);
            filterList.addFilter(filter);
        }else if (type == 3){
            SubstringComparator substringComparator = new SubstringComparator("3");
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"),
                    Bytes.toBytes("type"), CompareFilter.CompareOp.EQUAL,substringComparator);
            filterList.addFilter(filter);
        }else if (type == 4){
            SubstringComparator substringComparator = new SubstringComparator("4");
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"),
                    Bytes.toBytes("type"), CompareFilter.CompareOp.EQUAL,substringComparator);
            filterList.addFilter(filter);
        }

        //根据信息源名字查询
        SubstringComparator substringComparator3 = null;
        SingleColumnValueFilter filter3 = null;
        if (infoName!=null){
            substringComparator3 = new SubstringComparator(infoName);
            filter3 = new SingleColumnValueFilter(Bytes.toBytes("info"),
                    Bytes.toBytes("infoName"), CompareFilter.CompareOp.EQUAL,substringComparator3);
        }else if (infoSourceName!=null){
            substringComparator3 = new SubstringComparator(infoSourceName);
            filter3 = new SingleColumnValueFilter(Bytes.toBytes("info"),
                    Bytes.toBytes("infoSourceName"), CompareFilter.CompareOp.EQUAL,substringComparator3);
        }
        if (filter3!=null){
            filterList.addFilter(filter3);
        }

        SubstringComparator substringComparator2 =null;
        SingleColumnValueFilter filter2 = null;
        if (collectLevel!=null){
             substringComparator2 = new SubstringComparator(collectLevel.toString());
             filter2 = new SingleColumnValueFilter(Bytes.toBytes("info"),
                    Bytes.toBytes("collectLevel"), CompareFilter.CompareOp.EQUAL,substringComparator2);
        }
        if (filter2!=null){
            filterList.addFilter(filter2);
        }

        scan.setFilter(filterList);
        ResultScanner resultScanner = null;
        try {
            resultScanner = informationStatsTable.getScanner(scan);
            //根据昨日采集数据量判断，状态是否正常。
            if (infoState == 2){
                for(Result result : resultScanner){
                    long dayCount = result.containsColumn(infos,dayCounts)?Bytes.toLong(result.getValue(infos,dayCounts)):0L;
                    if (dayCount != 0){
                        count ++ ;
                        if (count > startRow && count <= startRow + pageSize) {
                            CollectCountBean collectCountBean = new CollectCountBean();
                            collectCountBean.setInfoName(Bytes.toString(result.getValue(infos,infoNames)));
                            collectCountBean.setInfoSourceName(Bytes.toString(result.getValue(infos,infoSourceNames)));
                            int colv  = IsNum.isNumeric(Bytes.toString(result.getValue(infos,collectLevels)))?Integer.parseInt(Bytes.toString(result.getValue(infos,collectLevels))):3;
                            collectCountBean.setCollectLevel(colv);
                            collectCountBean.setType(Bytes.toString(result.getValue(infos,types)));
                            collectCountBean.setTodayTotal(dayCount);
                            collectCountBean.setMonthTotal(Bytes.toLong(result.getValue(infos,counts)));
                            collectCountBean.setInfoState("正常");
                            JSONObject json = ParseJsonData.getJson(collectCountBean);
                            jsonArray.add(json);
                        }
                    }
                }
            }else if (infoState == 3){
                for(Result result : resultScanner){
                    long dayCount = result.containsColumn(infos,dayCounts)?Bytes.toLong(result.getValue(infos,dayCounts)):0L;
                    if (dayCount == 0){
                        count ++ ;
                        if (count > startRow && count <= startRow + pageSize) {
                            CollectCountBean collectCountBean = new CollectCountBean();
                            collectCountBean.setInfoName(Bytes.toString(result.getValue(infos,infoNames)));
                            collectCountBean.setInfoSourceName(Bytes.toString(result.getValue(infos,infoSourceNames)));
                            int colv  = IsNum.isNumeric(Bytes.toString(result.getValue(infos,collectLevels)))?Integer.parseInt(Bytes.toString(result.getValue(infos,collectLevels))):3;
                            collectCountBean.setCollectLevel(colv);
                            collectCountBean.setType(Bytes.toString(result.getValue(infos,types)));
                            collectCountBean.setTodayTotal(dayCount);
                            collectCountBean.setMonthTotal(Bytes.toLong(result.getValue(infos,counts)));
                            collectCountBean.setInfoState("异常");
                            JSONObject json = ParseJsonData.getJson(collectCountBean);
                            jsonArray.add(json);
                        }
                    }
                }
            }else {
                for(Result result : resultScanner){
                        count ++ ;
                        if (count > startRow && count <= startRow + pageSize) {
                            long dayCount = result.containsColumn(infos,dayCounts)?Bytes.toLong(result.getValue(infos,dayCounts)):0L;
                            CollectCountBean collectCountBean = new CollectCountBean();
                            collectCountBean.setInfoName(Bytes.toString(result.getValue(infos,infoNames)));
                            collectCountBean.setInfoSourceName(Bytes.toString(result.getValue(infos,infoSourceNames)));
                            int colv  = IsNum.isNumeric(Bytes.toString(result.getValue(infos,collectLevels)))?Integer.parseInt(Bytes.toString(result.getValue(infos,collectLevels))):3;
                            collectCountBean.setCollectLevel(colv);
                            collectCountBean.setType(Bytes.toString(result.getValue(infos,types)));
                            collectCountBean.setTodayTotal(dayCount);
                            collectCountBean.setMonthTotal(Bytes.toLong(result.getValue(infos,counts)));
                            if (dayCount == 0){
                                collectCountBean.setInfoState("异常");
                            }else {
                                collectCountBean.setInfoState("正常");
                            }
                            JSONObject json = ParseJsonData.getJson(collectCountBean);
                            jsonArray.add(json);
                        }
                    }
                }
            }
        catch (IOException e) {
            e.printStackTrace();
        }

        CountBean countBean = new CountBean(count);
        JSONObject countJson = ParseJsonData.getJson(countBean);
        jsonArray.add(countJson);
        //返回jsonString
        return jsonArray.toString();
    }


    @GetMapping("log/keyword")
    public String KeywordQuery(@RequestParam(value = "keyword",required = false)String keyword,
                               @RequestParam(value = "articleTitle",required = false)String articleTitle,
                               @RequestParam("beginTime")Long beginTime,
                               @RequestParam("endTime")Long endTime,
                               @RequestParam(value = "startKey",required = false)String startKey,
                               @RequestParam(value = "page",required = false,defaultValue="1")int page,
                               @RequestParam(value = "pageSize",required = false,defaultValue = "20")int pageSize){
        String tableName = "infoManager:keywordLog";
        HTable hTable = HBaseTableGet.getKeywordLog_v2(tableName);
        JSONArray jsonArray = new JSONArray();
        Integer count = 0;
        Scan scan = new Scan();
        try {
            scan.setTimeRange(beginTime,endTime);
        } catch (IOException e) {
            e.printStackTrace();
        }
        scan.setMaxVersions(1);
        if (startKey!=null){
            scan.setStartRow(Bytes.toBytes(startKey));
        }else if (keyword!=null){
            //获取rowkey开始
            scan.setStartRow(Bytes.toBytes(getRowKey(keyword)));
        }

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        //依据不同的传入参数，设置不同的过滤规则
        SingleColumnValueFilter filter = null;
        SingleColumnValueFilter KeywordContain = null;
        if (keyword!=null){
            scan.setStopRow(Bytes.toBytes(getRowKey(keyword)+"}"));
        }else if (articleTitle!=null){
            SubstringComparator substringComparator = new SubstringComparator(articleTitle);
            filter = new SingleColumnValueFilter(Bytes.toBytes("info"),
                    Bytes.toBytes("articleTitle"), CompareFilter.CompareOp.EQUAL,substringComparator);
        }
        // 如果上诉传入参数不为空即含有过滤条件，则设置过滤器
            if (KeywordContain!=null){
                filterList.addFilter(KeywordContain);
            }else if(filter!=null){
                filterList.addFilter(filter);
            }
            //记录rowKey的bean
            RowKeyBean rowKeyBean = new RowKeyBean();
            //添加页面过滤器
            PageFilter pf = new PageFilter(60L);
            filterList.addFilter(pf);
            scan.setFilter(filterList);
            try {
                ResultScanner resultScanner = hTable.getScanner(scan);

                for(Result result : resultScanner){
                    Cell[] cells = result.rawCells();
                    String rowKey = Bytes.toString(result.getRow());
                    if (count == 0){
                        rowKeyBean.setStartKey(rowKey);
                    }
                    count ++ ;
                    //依据传入的page和pageSize大小计算出需要显示的页面。
                    if ( count <=  pageSize){
                        KeyWordLogBean keyWordLogBean = new KeyWordLogBean();
                        for (Cell cell : cells) {
                            String field = Bytes.toString(CellUtil.cloneQualifier(cell));
                            String value = Bytes.toString(CellUtil.cloneValue(cell));
                            if(field.equals("keyword")  && !"".equals(value.trim())){
                                keyWordLogBean.setKeyword(value);
                            }else if(field.equals("articleTitle")  && !"".equals(value.trim())){
                                keyWordLogBean.setArticleTitle(value);
                            }else if(field.equals("crawlTime")){
                                Long crawlTime = Bytes.toLong(CellUtil.cloneValue(cell));
                                keyWordLogBean.setCrawlTime(crawlTime);
                            }else if(field.equals("articleUrl")  && !"".equals(value.trim())){
                                keyWordLogBean.setArticleUrl(value);
                            }
                        }
                        JSONObject json = ParseJsonData.getJson(keyWordLogBean);
                        jsonArray.add(json);
                    }
                    //TODO
                    if (count == pageSize+1){
                        rowKeyBean.setNextKey(rowKey);
                        break;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        JSONObject countJson = ParseJsonData.getJson(rowKeyBean);
        jsonArray.add(countJson);
        return jsonArray.toString();
    }

    @GetMapping("log/keyword/amount")
    public String keywordLogAmount(){
        HTable resultTable = HBaseTableGet.getKeywordCountResult("infoManager:keywordCountResult");
        JSONArray jsonArray = new JSONArray();
        Scan scan = new Scan();
        byte[] info = Bytes.toBytes("info");
        byte[] count = Bytes.toBytes("count");
        byte[] keyword = Bytes.toBytes("keyword");
        byte[] increment = Bytes.toBytes("increment");
        try {
            ResultScanner scanner = resultTable.getScanner(scan);
            for (Result result : scanner) {
                String keywords = Bytes.toString(result.getValue(info,keyword));
                Long keywordCount = result.containsColumn(info,count)?Bytes.toLong(result.getValue(info,count)):0L;
                Long incrementCount = result.containsColumn(info,increment)?Bytes.toLong(result.getValue(info,increment)):0L;
                jsonArray.add(ParseJsonData.getJson(new KeywordCountBean(keywords,keywordCount,incrementCount)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        int size = jsonArray.size();
        CountBean countBean = new CountBean(size);
        jsonArray.add(countBean);
        return jsonArray.toString();
    }


    @GetMapping("/log/infomation")
    public String LogQuery(@RequestParam(value="infoName",required = false)String infoName ,
                           @RequestParam(value = "infoSourceName",required = false)String infoSourceName,
                           @RequestParam(value = "articleTitle",required = false)String articleTitle,
                           @RequestParam("beginTime")Long beginTime,
                           @RequestParam("endTime")Long endTime,
                           @RequestParam(value = "startKey",required = false)String startKey,
                           @RequestParam(value = "pageSize",required = false,defaultValue = "20")int pageSize){
        String tableName = "infoManager:crawler_log";
        HTable hTable = HBaseTableGet.getCrawler_log(tableName);
        JSONArray jsonArray = new JSONArray();

        //开始行
        //Integer startRow = (page -1) * pageSize;
        Integer count = 0;  //计数

        Scan scan = new Scan();
        try {
            scan.setTimeRange(beginTime,endTime);
        } catch (IOException e) {
            e.printStackTrace();
        }
        scan.setMaxVersions(1);
        if (startKey!=null){
            scan.setStartRow(Bytes.toBytes(startKey));
        }else if (infoSourceName!=null){
            //获取rowkey开始
            scan.setStartRow(Bytes.toBytes(getRowKey(infoSourceName)));
        }

        //依据不同的传入参数，设置不同的过滤规则
        SubstringComparator substringComparator = null;
        SingleColumnValueFilter filter = null;
        if (infoName!=null){
            substringComparator = new SubstringComparator(infoName);
            filter = new SingleColumnValueFilter(Bytes.toBytes("info"),
                    Bytes.toBytes("infoName"), CompareFilter.CompareOp.EQUAL,substringComparator);
        }else if (infoSourceName!=null){
            substringComparator = new SubstringComparator(infoSourceName);
            filter = new SingleColumnValueFilter(Bytes.toBytes("info"),
                    Bytes.toBytes("infoSourceName"), CompareFilter.CompareOp.EQUAL,substringComparator);
        }else if (articleTitle!=null){
            substringComparator = new SubstringComparator(articleTitle);
            filter = new SingleColumnValueFilter(Bytes.toBytes("info"),
                    Bytes.toBytes("articleTitle"), CompareFilter.CompareOp.EQUAL,substringComparator);
        }
        //设置rowkey，设置当前上一页rowkey
        RowKeyBean rowKeyBean = new RowKeyBean();
        // 如果上诉传入参数不为空即含有过滤条件，则设置过滤器
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        if (filter!=null){
            filterList.addFilter(filter);
        }
        //添加页面过滤器
        PageFilter pf = new PageFilter(60L);
        filterList.addFilter(pf);
        scan.setFilter(filterList);
            try {
                ResultScanner resultScanner = hTable.getScanner(scan);
                for(Result result : resultScanner){
                    Cell[] cells = result.rawCells();
                    String rowKey = Bytes.toString(result.getRow());
                    if (count == 0){
                        rowKeyBean.setStartKey(rowKey);   //将当前页面开始行，当做下一页中的上一页参数（lastkey）
                    }
                    count ++ ;
                    //依据传入的page和pageSize大小计算出需要显示的页面。
                    if ( count <=  pageSize){
                        LogBean logBean = new LogBean();
                        for(Cell cell : cells){
                            //TODO 代码优化
                            String field = Bytes.toString(CellUtil.cloneQualifier(cell));
                            String value = Bytes.toString(CellUtil.cloneValue(cell));
                            if(field.equals("infoName")  && !"".equals(value.trim())){
                                logBean.setInfoName(value);
                            }else if(field.equals("infoSourceName")  && !"".equals(value.trim())){
                                logBean.setInfoSourceName(value);
                            }else if(field.equals("articleTitle")  && !"".equals(value.trim())){
                                logBean.setArticleTitle(value);
                            }else if(field.equals("crawlTime")){
                                Long crawlTime = Bytes.toLong(CellUtil.cloneValue(cell));
                                logBean.setCrawlTime(crawlTime);
                            }else if(field.equals("articleUrl")  && !"".equals(value.trim())){
                                logBean.setArticleUrl(value);
                            }
                        }
                        JSONObject json = ParseJsonData.getJson(logBean);
                        jsonArray.add(json);
                    }
                    if (count == pageSize+1){
                        rowKeyBean.setNextKey(rowKey); //获取下一页的startKey
                        break;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        JSONObject countJson = ParseJsonData.getJson(rowKeyBean);
        jsonArray.add(countJson);
        //返回jsonString
        return jsonArray.toString();
    }

    //获得关键词日志rowKey
    public static String getRowKey(String keyword){
        return Md5Utils.getMd5ByStr(keyword).substring(7, 23);
    }

    /**
     * 微博用户、微信用户、头条用户，根据name、province、city，进行模糊匹配。
     */
    @GetMapping("/user/information")
    public String userQuery(@RequestParam(value="name",required = false,defaultValue="")String name ,
                            @RequestParam(value = "sourceType")String sourceType,
                            @RequestParam("startRow")Long startRow,
                            @RequestParam("size")Long size,
                            @RequestParam(value = "province",required = false,defaultValue="")String province,
                            @RequestParam(value = "city",required = false,defaultValue="")String city,
                            @RequestParam(value = "mediaType",required = false,defaultValue="")String mediaType){
        return userService.userRegexQuery(name, sourceType, startRow, size, province, city,mediaType);
    }

    /**
     * 添加前端搜索词，接收形式JSON数组接收
     */
    @RequestMapping(value = "/keywords/adding",method = RequestMethod.POST)
    public String addKeyword(@RequestBody JSONObject params){
        return userService.addKeyword(params);
    }

    /**
     * 获取某个知道用户id的账号的具体信息
     */
    @GetMapping("/userInfo/getting")
    public String LogQuery(@RequestParam(value="tableName")String tableName ,
                           @RequestParam(value = "id")String id){
        return userService.getUserInfo(tableName,id);
    }

// 源管理平台代码

    /**
     *  添加自媒体信息
     */
    @RequestMapping(value = "/media/log/adding",method = RequestMethod.POST)
    public String addMediaLog(@RequestBody JSONArray params){
        String  flag = "插入失败，请检查mid";
            //TODO 需要将测试表换为正式表
            Table table = HBaseGeneral.getTable("insertTest2");
            ArrayList<Put> putList = new ArrayList<Put>();
        try {
            for (int i = 0; i < params.size(); i++) {
                JSONObject jsonObject = params.getJSONObject(i);
                //TODO 插入MD5 mid
                String mid = jsonObject.containsKey("") ? jsonObject.getString("") : "";
                Put put = new Put(Bytes.toBytes(Md5Utils.getMd5ByStr(mid)));
                //TODO 需要插入字段
                put = HBaseGeneral.jsonObject2Put(jsonObject, "info", put, "");
                putList.add(put);
            }
            HBaseGeneral.tableDataWrite(table,putList);
            flag = "插入成功";
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            HBaseGeneral.tableClose(table);
            return flag;
        }
    }
}


