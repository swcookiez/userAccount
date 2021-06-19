package com.izhonghong.huizhou.concentric.data_store

import java.lang
import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSONObject
import com.izhonghong.common.alijson.ParseJsonData
import com.izhonghong.huizhou.concentric.bean.Crawler
import com.izhonghong.huizhou.concentric.util.HiveUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author sw
 * sparkStreaming 实时消费kafka数据流入hive
 * 2021/1/6  15:52
 * todo:优化方向 采集信息为最大格式，额外信息为一个字段，前端获取自行解析。可建立一张码表，使用id进行关联。
 * todo:榜单数据都用一个topic,写入ods层。ods解析平台，导出不同主题的dwd层表，dwd层表计算各主题榜单。扫描表导出
 */
object CrawlData2Hive {
  val formatterDay = new SimpleDateFormat("yyyyMMdd")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("惠州同心圆-sparkStreaming:"+this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "2000") //todo 10000
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")

    val ssc = new StreamingContext(conf, Seconds(60)) //todo 继续调大避免产生大量小文件 5min
    val sparkContext = ssc.sparkContext
    sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    sparkContext.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    val topics = Array("huizhou_concentric_crawl_return")
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "10.248.161.20:9092,10.248.161.21:9092,10.248.161.22:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-test",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    val kfStream: InputDStream[ConsumerRecord[String, String]] =  KafkaUtils.createDirectStream(
      ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String, String](topics, kafkaMap)
    )

    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import sparkSession.implicits._
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtil.setMaxpartitions(sparkSession) //设置最大分区数
    HiveUtil.openCompression(sparkSession) //开启压缩
    HiveUtil.useSnappyCompression(sparkSession) //使用snappy压缩
    kfStream.map(ele=>ele.value()).mapPartitions(partition => partition.map(ele=>{
      jsonConvert(ParseJsonData.getJsonData(ele))
    })).foreachRDD(rdd => {
      rdd.toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("huizhou.ods_concentric_crawl")
    })

    //手动提交offset
    kfStream.foreachRDD(
      rdd => {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        kfStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)//手动提交offset
      })

    ssc.start()
    ssc.awaitTermination()
  }

    def jsonConvert(json:JSONObject): Crawler ={
        val dataJson = json.getJSONArray("data").getJSONObject(0)
        val deptId = json.getString("deptId")
        val deptName = json.getString("deptName")
        val id = json.getString("id")
        val mediaName = json.getString("mediaName")
        val mediaType = json.getString("mediaType")
        val crawler_time = dataJson.getLongValue("crawler_time")
        val created_at = dataJson.getLongValue("created_at")
        val mid = dataJson.getString("mid")
        val name = dataJson.getString("name")
        val read_count = dataJson.getIntValue("read_count")
        val text = dataJson.getString("text")
        val title = dataJson.getString("title")
        val uid = dataJson.getString("uid")
        val url = dataJson.getString("url")
        val zan_count = dataJson.getIntValue("zan_count")
        val followers_count = dataJson.getIntValue("followers_count")
        val created_day: String = formatterDay.format(created_at)
        Crawler(deptId,deptName,id,mediaName,mediaType,crawler_time,created_at,mid,name,read_count,text,title,uid,url,zan_count,followers_count,created_day)
    }
}