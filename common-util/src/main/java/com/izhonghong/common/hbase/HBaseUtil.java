package com.izhonghong.common.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Hbase工具类
 */
@Deprecated
public class HBaseUtil {
    static Connection connection;
    static Configuration conf;
    //初始化HBase连接 TODO: 尝试写入配置文件中
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "fwqml016.zh,fwqml001.zh,fwqml009.zh");
        //conf.set("hbase.zookeeper.quorum", "192.168.2.113,192.168.2.114,192.168.2.116");
        //conf.set("hbase.zookeeper.property.clientPort","2183");
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取表连接
     * @param tableName 表名
     */
    public static Table getTable(String tableName){
        Table table = null ;
        try {
            table  = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
            table = null;
        }
        return table;
    }

    /**
     * 为HBase scan添加所需要扫描的列
     * @param scan HBase table扫描器
     * @param fn  列族
     * @param columns 列
     * @return scan：添加列后的扫描器
     */
    public static Scan addScanColumns(Scan scan,String fn,String ... columns){
        byte[] bytesFn = Bytes.toBytes(fn);
        for (String column : columns) {
            scan.addColumn(bytesFn,Bytes.toBytes(column));
        }
        return scan;
    }

    /**
     * 添加一行数据所需要的列
     * @param put HBase 添加数据的基本单位
     * @param fn  列族
     * @param columns 列
     * @return put
     */
    public static Put addPutColumns(Put put,String fn,String [] columns ,String ... values){
        byte[] bytesFn = Bytes.toBytes(fn);
        for (int i = 0; i < columns.length; i++) {
            if(!values[i].equals("")) put.addColumn(bytesFn,Bytes.toBytes(columns[i]),Bytes.toBytes(values[i]));
        }
        return put;
    }

    public static Put addPutColumns(Put put,String fn,String [] columns ,int ... values){
        byte[] bytesFn = Bytes.toBytes(fn);
        for (int i = 0; i < columns.length; i++) {
            put.addColumn(bytesFn,Bytes.toBytes(columns[i]),Bytes.toBytes(values[i]));
        }
        return put;
    }
}











