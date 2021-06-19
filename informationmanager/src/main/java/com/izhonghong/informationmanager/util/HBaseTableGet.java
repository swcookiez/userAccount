package com.izhonghong.informationmanager.util;

import com.izhonghong.hbase.common.util.HbClient;
import org.apache.hadoop.hbase.client.HTable;

public class HBaseTableGet {
    private static HTable infoSourceCountResult;
    private static HTable keywordLog_v2;
    private static HTable keywordCountResult;
    private static HTable crawler_log;

    //获取单例表
    public static HTable getInfoSourceCountResult(String tableName){
        if (infoSourceCountResult ==null){
            infoSourceCountResult= HbClient.getHTable(tableName);
        }
        return infoSourceCountResult;
    }

    public static HTable getKeywordLog_v2(String tableName){
        if (keywordLog_v2 ==null){
            keywordLog_v2= HbClient.getHTable(tableName);
        }
        return keywordLog_v2;
    }

    public static HTable getKeywordCountResult(String tableName){
        if (keywordCountResult ==null){
            keywordCountResult= HbClient.getHTable(tableName);
        }
        return keywordCountResult;
    }

    public static HTable getCrawler_log(String tableName){
        if (crawler_log ==null){
            crawler_log= HbClient.getHTable(tableName);
        }
        return crawler_log;
    }
}
