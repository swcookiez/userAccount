package com.izhonghong.spark.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * 2021/3/8 13:34
 * @author sw
 * spark工具类，读写HBase表。
 */
object SparkReadHBaseUtil {

  /**
   * HBase 配置设置，每张表各自独立
   * @param tableName 表名
   * @param operationType 操作类型,两种 read：读 | write：写
   * @return hBaseConf
   */
  def HBaseConfGet(tableName:String,operationType:String): Configuration ={
    val hBaseConf: Configuration = HBaseConfiguration.create()
    hBaseConf.setInt("hbase.rpc.timeout",200000)
    hBaseConf.setInt("hbase.client.operation.timeout", 200000)
    hBaseConf.setInt("hbase.client.scanner.timeout.period", 200000)
    hBaseConf.set("hbase.zookeeper.quorum","fwqml016.zh,fwqml001.zh,fwqml009.zh")
    hBaseConf.set(TableInputFormat.SHUFFLE_MAPS, "true")
    if("read".equals(operationType)){
      hBaseConf.set(TableInputFormat.INPUT_TABLE,tableName)
    }else if("write".equals(operationType)){
      hBaseConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
    }
    hBaseConf
  }

  /**
   * 读取HBase表，返回RDD
   * @param columns 需要读取的列
   * @param scan 扫描器
   * @return RDD
   */
  def readHBase(sc:SparkContext,hBaseConf:Configuration,columns:Array[String],scan: Scan) :RDD[(ImmutableBytesWritable, Result)] ={
    scan.setCaching(3000)
    for (elem: String <- columns) {
      scan.addColumn(Bytes.toBytes("fn"),Bytes.toBytes(elem))
    }
    val scanStr = Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
    hBaseConf.set(TableInputFormat.SCAN,scanStr)
    val hBaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]
    )
    hBaseRDD
  }

  /**
   * @param hBaseRDD 需要将原先RDD转为RDD(new ImmutableBytesWritable(),put)的形式，以便于写入HBase。
   */
  def writeHBase(hBaseConf:Configuration,hBaseRDD: RDD[(ImmutableBytesWritable, Put)]): Unit ={
    val job = Job.getInstance(hBaseConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    hBaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}