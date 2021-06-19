package com.izhonghong.huizhou.concentric.stat_return

import com.alibaba.fastjson.JSONObject
import com.izhonghong.common.kafka.KafkaProducerUtil
import com.izhonghong.huizhou.concentric.bean.RankList
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}



/**
 * @author sw
 * 2021/1/11  16:07
 */
object StatReturn {

  def main(args: Array[String]): Unit = {
    //System.setProperty("HADOOP_USER_NAME", "zhonghong") oozie调度会出现权限问题
    val sparkConf = new SparkConf().setAppName("Export-ads_concentric_crawl")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc: SparkContext = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    returnStatResult(sparkSession) //todo 可扩展，使用sql,传入样例类。
  }

  def returnStatResult(sparkSession: SparkSession): Unit ={
    val producer = KafkaProducerUtil.getProducer
    import sparkSession.implicits._
    val ads_concentric_crawl = sparkSession.sql(
      """
        |select *
        |from
        |huizhou.ads_concentric_crawl
        |""".stripMargin).as[RankList].collect() //ads层数据量较少，可拉到driver端处理

    for (elem <- ads_concentric_crawl) {
      producer.send(new ProducerRecord[String,String]("stat-test",toJson(elem).toJSONString))
    }
    producer.close()
  }

  def toJson(elem:RankList): JSONObject ={
    val nObject = new JSONObject()
    nObject.put("dept_id",elem.deptId)
    nObject.put("dept_name",elem.deptName)
    nObject.put("id",elem.id)
    nObject.put("media_name",elem.mediaName)
    nObject.put("media_type",elem.mediaType)
    nObject.put("article_number_day",elem.article_number_day)
    nObject.put("article_number_week",elem.article_number_week)
    nObject.put("article_number_month",elem.article_number_month)
    nObject.put("reading_number_day",elem.reading_number_day)
    nObject.put("reading_number_week",elem.reading_number_week)
    nObject.put("reading_number_month",elem.reading_number_month)
    nObject.put("reading_number_ave_day",elem.reading_number_ave_day)
    nObject.put("reading_number_ave_week",elem.reading_number_ave_week)
    nObject.put("reading_number_ave_month",elem.reading_number_ave_month)
    nObject.put("thumb_up_number_day",elem.thumb_up_number_day)
    nObject.put("thumb_up_number_week",elem.thumb_up_number_week)
    nObject.put("thumb_up_number_month",elem.thumb_up_number_month)
    nObject.put("fans_number_day",elem.fans_number_day)
    nObject.put("create_time",elem.created_day)
    nObject
  }
}






