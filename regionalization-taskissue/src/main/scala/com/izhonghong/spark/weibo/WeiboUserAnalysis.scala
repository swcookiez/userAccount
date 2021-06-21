package com.izhonghong.spark.weibo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author sw
 * @date 2020/11/23  13:33
 * 过滤出最近一个月发文过的用户ID
 */


object WeiboUserAnalysis {
  case class VtWeibo(last_post_time:String)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("weiboAccount_post_time_stats")

    val sc = new SparkContext(conf)
    val hBaseConf: Configuration = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum","fwqml016.zh,fwqml001.zh,fwqml009.zh")
    hBaseConf.set(TableInputFormat.INPUT_TABLE, "crawl:weibo_user_regionalization")

    //设置hbase 超时时间
    hBaseConf.setInt("hbase.rpc.timeout", 200000)
    hBaseConf.setInt("hbase.client.operation.timeout", 200000)
    hBaseConf.setInt("hbase.client.scanner.timeout.period", 200000)
    //hBaseConf.set(TableInputFormat.SHUFFLE_MAPS, "true")

    val scan = new Scan()
    scan.setCaching(3000)
    //scan.setTimeRange(1577808008000L, 1608182842714L) //获取时间戳范围内的数据1月1日 - 12月17日
    scan.addColumn(Bytes.toBytes("fn"),Bytes.toBytes("last_post_time"))
    scan.setMaxVersions(1)


    val scanStr = Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
    hBaseConf.set(TableInputFormat.SCAN,scanStr)
    //val job = getJob()

    val weiboRDD = sc.newAPIHadoopRDD(
      hBaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    //一个月以前的发布时间
    val oneMonthBeforePostTime = System.currentTimeMillis() - 2592000000L

    //weiboRDD.persist(StorageLevel.MEMORY_AND_DISK)   //缓存RDD,当单分区超过2G，会报异常。需要repartition扩大分区数。不缓存时则不会报异常。

    val result: Long = weiboRDD.count()

    val filterResult: Long = weiboRDD.map({
      case (_,result) =>
        val last_post_time = if(result.containsColumn(Bytes.toBytes("fn"),Bytes.toBytes("last_post_time"))) Bytes.toString(result.getValue(Bytes.toBytes("fn"),Bytes.toBytes("last_post_time"))) else "00000000"
          val time = last_post_time.toLong
        time
    }).filter(v => v > oneMonthBeforePostTime).count()

    println("拥有最新发文时间标记的账号数: "+result)
    println("最近一个月发文的账号数      : "+filterResult)

    sc.stop()
  }

}
