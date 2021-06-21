package com.izhonghong.spark.weibo

import com.izhonghong.regionalization.util.Md5Utils
import com.izhonghong.spark.util.SparkReadHBaseUtil
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author sw
 * deploy
 * 导出微博活跃用户
 */
object WeiboActiveUserExport {
  def main(args: Array[String]): Unit = {
//  1.创建spark上下文环境
    val conf = new SparkConf().setAppName("weiboActiveExport")
    val sc: SparkContext = new SparkContext(conf)
//  2.获取读取表的hBaseConf,获取HBaseRDD
    val hBaseConf = SparkReadHBaseUtil.HBaseConfGet("crawl:weibo_user_regionalization", "read")
    val weiboRDD: RDD[(ImmutableBytesWritable, Result)] = SparkReadHBaseUtil.readHBase(sc, hBaseConf, Array("last_post_time","uid"),new Scan())

//  3.2.map转换
    val writeRDD = weiboRDD.coalesce(200).map({
      case (_,result) =>
        parseResult(result)
    }).filter(item => {
      val timeStamp = item._1.toLong
      timeStamp > (System.currentTimeMillis() - 3456000000L)
    }).map(item => {
      val put = new Put(Bytes.toBytes(Md5Utils.getMd5ByStr(item._2)))
      put.addColumn(Bytes.toBytes("fn"),Bytes.toBytes("last_post_time"),Bytes.toBytes(item._1))
      put.addColumn(Bytes.toBytes("fn"),Bytes.toBytes("uid"),Bytes.toBytes(item._2))
      (new ImmutableBytesWritable(),put)
    })
//  4.写入hBase表
//  4.1 获取写入hBase配置
    val hBaseConfWrite = SparkReadHBaseUtil.HBaseConfGet("crawl:weibo_active_user", "write")
    SparkReadHBaseUtil.writeHBase(hBaseConfWrite,writeRDD)
//  5 释放资源
    sc.stop()
  }


  def parseResult(result:Result): (String, String) ={
    val fn = Bytes.toBytes("fn")
    val last_post_time = if(result.containsColumn(fn,Bytes.toBytes("last_post_time"))) Bytes.toString(result.getValue(fn,Bytes.toBytes("last_post_time"))) else "0"
    val uid = if(result.containsColumn(fn,Bytes.toBytes("uid"))) Bytes.toString(result.getValue(fn,Bytes.toBytes("uid"))) else ""
    (last_post_time,uid)
  }
}
