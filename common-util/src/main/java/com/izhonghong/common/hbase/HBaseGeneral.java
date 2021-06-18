package com.izhonghong.common.hbase;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * HBase 新版自定义工具
 */
public class HBaseGeneral {
    static Connection connection;
    static Configuration conf;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "fwqml016.zh,fwqml001.zh,fwqml009.zh");
        conf.setInt("hbase.rpc.timeout", 600000);
        conf.setInt("hbase.client.operation.timeout", 600000);
        conf.setInt("hbase.client.scanner.timeout.period", 600000);
        //conf.set("hbase.zookeeper.quorum", "test217");
        /*conf.set("hbase.zookeeper.quorum", "192.168.2.113,192.168.2.114,192.168.2.116");
        conf.set("hbase.zookeeper.property.clientPort","2183");*/
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取连接
     */
    public static Table getTable(String tableName){
        Table table = null ;
        try {
            table  = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

// 以上是初始化代码***************************************分割线**************************************************


    /**
     * 为HBase scan添加所需要扫描的列
     */
    public static Scan addScanColumns(Scan scan,String fn,String ... columns){
        byte[] bytesFn = Bytes.toBytes(fn);
        for (String column : columns) {
            scan.addColumn(bytesFn,Bytes.toBytes(column));
        }
        return scan;
    }

    /**
     * @return ResultScanner 返回结果扫描器
     */
    public static ResultScanner getScanner(Table table,Scan scan){
        ResultScanner scanner = null ;
        try {
            scanner = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return scanner;
    }

    /**
     * 将Scan中的result转化为jsonString
     * @return jsonString
     */
    public static String result2JsonStr(Result result,String fn,String ... fields){
        JSONObject jsonObject = new JSONObject();
        for (String field : fields) {
            String colValue = getHBaseColValue(result,fn,field);
            jsonObject.put(field,colValue);
        }
        return jsonObject.toString();
    }

    /**
     * 获取HBASE表中列的值
     * @param fn 列族
     * @param columnName 列名
     * @return 列值
     */
    public static String getHBaseColValue(Result result,String fn,String columnName){
        return result.containsColumn(Bytes.toBytes(fn), Bytes.toBytes(columnName))
                ?Bytes.toString(result.getValue(Bytes.toBytes(fn), Bytes.toBytes(columnName))):"";
    }

    /**
     * 处理String 字段类型，解析json并添加Put
     * @param fn  列族
     * @param put 事先定义好put传入,解析jsonObject的某个字段，将其做加密等处理后作为rowKey，传入。
     */
    public static Put jsonObject2Put(JSONObject jsonObject,String fn,Put put,String ... fields){
        byte[] bytesFn = Bytes.toBytes(fn);
        for (String field : fields) {
            String value = jsonObject.containsKey(field) ? jsonObject.getString(field) : "";
            if (!"".equals(value) && value!=null) put.addColumn(bytesFn,Bytes.toBytes(field),Bytes.toBytes(value));
        }
        return  put;
    }

    /**
     * 将putList数据写入HBase表中，并清空PutList
     */
    public static void tableDataWrite(Table table,ArrayList<Put> putList){
        try {
              table.put(putList);
              putList.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 关闭表代码
     */
    public static void tableClose(Table table){
        try {
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
