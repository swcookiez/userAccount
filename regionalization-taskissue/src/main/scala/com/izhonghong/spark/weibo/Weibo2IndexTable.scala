package com.izhonghong.spark.weibo

import com.izhonghong.spark.util.{MyESUtil, SparkReadHBaseUtil}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 2021/3/8 11:30
 *
 * @author sw
 * deploy
 */
object Weibo2IndexTable {

  case class Weibo(uid: String, province: String, city: String, name: String)

  def main(args: Array[String]): Unit = {
//  1.创建spark上下文环境
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)
//  2.获取读取表的hBaseConf,获取HBaseRDD
    val hBaseConf = SparkReadHBaseUtil.HBaseConfGet("crawl:weibo_user_regionalization", "read")
    val weiboRDD: RDD[(ImmutableBytesWritable, Result)] = SparkReadHBaseUtil.readHBase(sc, hBaseConf, Array("uid","province","city","name"),new Scan()) // 参数过多可考虑封装对象传递
//  3.hBase具体操作
    weiboRDD.map({
      case (_,result) =>
        val item = parseResult(result) //("uid","province","city","name")
        Weibo(item._1,item._2,item._3,item._4)
    }).foreachPartition(
      partition =>{
        MyESUtil.insertBulk("weibo_user_regionalization","weibo_user_regionalizationtype",partition.toList)
      }
    )
    sc.stop()
  }

  /**
   * result 解析
   * @return ("uid","province","city","name")
   */
  def parseResult(result:Result): (String, String, String, String) ={
    val fn = Bytes.toBytes("fn")
    val province = if(result.containsColumn(fn,Bytes.toBytes("province"))) Bytes.toString(result.getValue(fn,Bytes.toBytes("province"))) else ""
    val uid = if(result.containsColumn(fn,Bytes.toBytes("uid"))) Bytes.toString(result.getValue(fn,Bytes.toBytes("uid"))) else ""
    val city = if(result.containsColumn(fn,Bytes.toBytes("city"))) Bytes.toString(result.getValue(fn,Bytes.toBytes("city"))) else ""
    val name = if(result.containsColumn(fn,Bytes.toBytes("name"))) Bytes.toString(result.getValue(fn,Bytes.toBytes("name"))) else ""
    (uid,province,city,name)
  }

}
