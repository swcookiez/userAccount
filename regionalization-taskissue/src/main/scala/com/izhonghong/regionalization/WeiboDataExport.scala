package com.izhonghong.regionalization

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 微博数据量最大月份导出
 */
object WeiboDataExport {
  val fn = Bytes.toBytes("fn")

  def main(args: Array[String]): Unit = {
    weiboExport()
  }

  def weiboExport(): Unit ={
    val conf = new SparkConf().setAppName("weiboData-export-APP")
      //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //设置kryo序列化
      // 注册需要使用 kryo 序列化的自定义类
      //.registerKryoClasses(Array(classOf[MyCsvWriter]))

    val sc = new SparkContext(conf)
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum","fwqml016.zh,fwqml001.zh,fwqml009.zh")
    hBaseConf.set(TableInputFormat.INPUT_TABLE, "zh_ams_ns:vt_weibo")
    //设置hbase 超时时间
    hBaseConf.setInt("hbase.rpc.timeout", 200000)
    hBaseConf.setInt("hbase.client.operation.timeout", 200000)
    hBaseConf.setInt("hbase.client.scanner.timeout.period", 200000)
    hBaseConf.set(TableInputFormat.SHUFFLE_MAPS, "true") //此参数可以研究

    val scan = new Scan()
    scan.setCaching(3000)
    scan.setTimeRange(1582992001000L, 1586158811000L) //获取时间戳范围内的数据3月1日 - 3月2日 todo 测试

    scan.setMaxVersions(1)
    val scanStr = Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
    hBaseConf.set(TableInputFormat.SCAN,scanStr)


    //获取kafka连接
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", "10.248.161.20:9092,10.248.161.21:9092,10.248.161.22:9092")
        p.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p
      }
      sc.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    val weiboRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hBaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    ).coalesce(1000)

    //不使用mapPartitions的原因，可以能会有数据倾斜，导致oom。
    weiboRDD.map{
      case (_,result) =>
        val created_date = if(result.containsColumn(Bytes.toBytes("fn"),Bytes.toBytes("created_date"))) Bytes.toString(result.getValue(Bytes.toBytes("fn"),Bytes.toBytes("created_date"))) else "00000000"
        (created_date.substring(0,6),result)
    }.filter(kv => kv._1.equals("202003"))/*.map{
      case (_,result) =>
        csvProcess(result,producer)
    }.take(1)*/.foreach(kv => {
      val result = kv._2
      val kafkaSink: KafkaSink[String, String] = kafkaProducer.value  //此时各个节点，已经拥有词变量。
      csvProcess(result,kafkaSink)
    })

    sc.stop()
  }

  //执行csv文件写入
  def csvProcess(result: Result,kafkaSink: KafkaSink[String, String]): Unit ={
    val text = resultGetValue(result,"text")
    val name = resultGetValue(result,"name")
    val reposts_count = resultGetValue(result,"reposts_count")
    val uid = resultGetValue(result,"uid")
    val sourceMid = resultGetValue(result,"sourceMid")
    val mid_p = resultGetValue(result,"mid_p")
    val zans_count = resultGetValue(result,"zans_count")
    val pic = resultGetValue(result,"pic")
    val read_count = resultGetValue(result,"read_count")
    val weibo_url = resultGetValue(result,"weibo_url")
    val comments_count = resultGetValue(result,"comments_count")
    val created_at = resultGetValue(result,"created_at")
    val crawler_time = resultGetValue(result,"crawler_time")

    val json = new JSONObject()
    /**
     * {"text","name","reposts_count","uid","sourceMid","mid_p","zans_count","pic","read_count","weibo_url",
     * "comments_count","created_at","crawler_time"};
     */
    json.put("text",text);
    json.put("name",name);
    json.put("reposts_count",reposts_count);
    json.put("uid",uid);
    json.put("sourceMid",sourceMid);
    json.put("mid_p",mid_p);
    json.put("zans_count",zans_count);
    json.put("pic",pic);
    json.put("read_count",read_count);
    json.put("weibo_url",weibo_url);
    json.put("comments_count",comments_count);
    json.put("created_at",created_at)
    json.put("crawler_time",crawler_time)
    //kafka 发送数据时，各节点才开始创建KafkaProducer对象。整个过程中KafkaProducer都无需被序列化和反序列化。
    kafkaSink.send("weiboData-export2",json.toJSONString)//send(new ProducerRecord("weiboData-export",json.toJSONString))
  }

  //解析result
  def resultGetValue(result: Result,field:String): String ={
    val value = if(result.containsColumn(fn,Bytes.toBytes(field))) Bytes.toString(result.getValue(fn,Bytes.toBytes(field))) else ""
    value
  }
}
