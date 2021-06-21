package com.izhonghong.regionalization

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author sw
 * @date 2020/11/23  13:33
 */


object WeiboAccountCount {
  case class VtWeibo(weibo_url:String)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("weiboAccountCount")
    val sc = new SparkContext(conf)
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum","fwqml016.zh,fwqml001.zh,fwqml009.zh")
    hBaseConf.set(TableInputFormat.INPUT_TABLE, "crawl:weibo_user_regionalization")
    //设置hbase 超时时间
    hBaseConf.setInt("hbase.rpc.timeout", 200000)
    hBaseConf.setInt("hbase.client.operation.timeout", 200000)
    hBaseConf.setInt("hbase.client.scanner.timeout.period", 200000)
    hBaseConf.set(TableInputFormat.SHUFFLE_MAPS, "true")

    val scan = new Scan()
    scan.setCaching(3000)
    //scan.setTimeRange(1577808008000L, 1608182842714L) //获取时间戳范围内的数据1月1日 - 12月17日
    //scan.addColumn(Bytes.toBytes("fn"),Bytes.toBytes("verified_type"))
    //scan.setMaxVersions(1)


    val scanStr = Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
    hBaseConf.set(TableInputFormat.SCAN,scanStr)

    val weiboRDD = sc.newAPIHadoopRDD(
      hBaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    ).repartition(500)

    //weiboRDD.persist(StorageLevel.MEMORY_AND_DISK)   //缓存RDD

    val result = weiboRDD.map{
      case (_,result) =>
        val verified_type = if(result.containsColumn(Bytes.toBytes("fn"),Bytes.toBytes("verified_type"))) Bytes.toString(result.getValue(Bytes.toBytes("fn"),Bytes.toBytes("verified_type"))) else "00000000"
       verified_type
    }.filter(x => x.equals("1") || x.equals("2")).groupBy(x => x).map(kv => {
      (kv._1,kv._2.size)
    }).collect()
    for (elem <- result) {
      println("类型："+elem._1+"\t"+elem._2)
    }

    sc.stop()
  }

  def getJob(): Job ={
    //spark连接hbase相关配置
    val hbaseConf: Configuration = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "fwqml016.zh,fwqml001.zh,fwqml009.zh")
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE,"insertTest")
    //通过job设置输出的格式的类
    val job: Job = Job.getInstance(hbaseConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])
    job
  }
}
