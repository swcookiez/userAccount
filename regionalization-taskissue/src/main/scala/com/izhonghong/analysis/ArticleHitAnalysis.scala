package com.izhonghong.analysis

import java.util.Properties
import com.alibaba.fastjson.JSONObject
import com.izhonghong.regionalization.KafkaSink
import com.izhonghong.spark.util.SparkReadHBaseUtil
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 2021/6/8 13:27
 * @author sw
 * deploy
 */
object ArticleHitAnalysis {
	def main(args: Array[String]): Unit = {
		//  1.创建spark上下文环境
		val conf = new SparkConf().setAppName("weiboMediaExport")
				.set("spark.default.parallelism", "500")
				.set("spark.locality.wait", "10")
		val sc: SparkContext = new SparkContext(conf)
		//  2.获取读取表的hBaseConf,获取HBaseRDD
		val hBaseConf = SparkReadHBaseUtil.HBaseConfGet("zh_ams_ns:wechat_article", "read")

		val hBaseRDD: RDD[(ImmutableBytesWritable, Result)] = SparkReadHBaseUtil.readHBase(sc, hBaseConf, Array("uid", "text"),new Scan()) // 参数过多可考虑封装对象传递
		val classify1 = "创新发展研究院|博雅文化研究基金会|当代社会观察研究所|东突|法轮功|绿色蔷薇女工服务中心|市基层色素摄影发展中心|重低音工人乐队|女权之声|全国旅游群|送饭活动|弘爱阅读推广中心|行家茶道会|心灵法门|万民中央圣洁教会|韩国基督教马克楼|耶和华证人|香港福音证主协会|全球华人基督徒律师峰会|正道企业管理咨询有限公司|英国驻领导办公司|对华援助协会|中国劳工透视|胡嘉岷|刘晓波|刘开明|庄慎之|王瑛|宣传宪政|马仕健|伊力哈木艾合买提|俄加斯|阿扎提江|安创集团|朱敏|律师郭维宏|香港|周保松|女权之声|黄沙|克里斯托弗鲍尔丁|段毅|秀才江湖|魏承恩|张治儒|全民共振|李一平|吴建明|送饭活动|刘春林|周筱林|民间修史|百年中国历史|南亚国情研究|六四活动|修宪|反终身制|改革开放四十周年|改革开放40周年|女工摄影展|全国首届女工艺术节|平易近人世界巡回演唱会|志奋领|全民共振|十一共振|段友共振|拯救民运分子|吴斌|秀才江湖|作者|黄美娟|贾榀|新天地教会|约翰支派上海支教会开辟地|市天宇星辰科技有限公司|国际女性和平团体|支部|市世界和平协会|市世界和平促进会|李在勇|邸亚敏|广西|陈家鸿|声援全民爆料人|被迫害的公民|广西异议分子|彭文斌|艺博会|高炬|香港|医院|千人计划|科技部|王志刚|陈胜|王跃|粉丝团|南方街头|街头政治|涂污|非转农|杨汉军|尘肺病|云集品|华泰小区|旧改|东山小区|泼墨|网约车|工会|朝日新闻|渡边雅隆|庞红振|亚洲漫画大展|启蒙者石汉瑞|当代社会观察研究所|所长刘开明|达洼宣教团|伊斯兰|五一共振|娃娃计划|佳士|工人|工友|维权|上访|八九|政治风波|535|文字狱"
		val classify2 = "中兴|芯片|华为|台山核电站|世界政法大会|文博会|全民共振|南周相亲会|周保松|女权之声|佳士|声援团|临聘|临时|老师|教师|合同工|退伍军人|千人计划|高敬德|改革开放|展览馆|广东|南下|南巡|习近平|港珠澳大桥|习近平|外国记者会|FCC|马凯|香港|入境|港府|拒签|港独|封杀|马建|异见作家|香港|港府|中国梦|大馆|文学节|港独|巴丢草|香港|港府|漫画家|画展|反动漫画|坦克人|港独|巴丢草事件|马建事件|马凯事件|党群服务中心|智慧党建|社区专职工作者|党建组织员|党群服务中心|智慧党建|社区专职工作者|党建组织员|错时服务|延时服务|曹长青|徐敬亚|孟浪|惠州核电|太平岭核电|CECA|单增辉|中国劳工通讯|中国维权|盛可以|前海金牛|私募|财产申报|个人事项|范伟军|龙岗|看守所|监狱|劳改所|谢晓蓉|高郭民|兴森|F35|军工"

		val c1: Broadcast[String] = sc.broadcast(classify1)
		val c2: Broadcast[String] = sc.broadcast(classify2)
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

		hBaseRDD.repartition(100).map({
			case (_,result) =>
				parseResult(result)
		}).groupByKey(100)
        				.map(kv => {
							var str = ""
							kv._2.foreach(item => {
								str = str + item + " "
							})
							(kv._1,str)
						})
        				.map(kv =>{
							val type1json = getHitCount("type1Count", kv._2, c1.value)
							val type2json = getHitCount("type2Count", kv._2, c2.value)
							val nObject = new JSONObject()
							nObject.put("uid",kv._1)
							nObject.put("type1Count",type1json)
							nObject.put("type2Count",type2json)
							nObject
						})
        				.foreach(json => {
							val kafkaSink: KafkaSink[String, String] = kafkaProducer.value
							kafkaSink.send("type-hit",json.toJSONString)
						})
		sc.stop()
	}

	def parseResult(result:Result): (String, String) ={
		val fn = Bytes.toBytes("fn")
		val uid = if(result.containsColumn(fn,Bytes.toBytes("uid"))) Bytes.toString(result.getValue(fn,Bytes.toBytes("uid"))) else ""
		val text = if(result.containsColumn(fn,Bytes.toBytes("text"))) Bytes.toString(result.getValue(fn,Bytes.toBytes("text"))) else ""
		(uid,text)
	}

	def getHitCount(typeName:String,text:String,typeString:String): JSONObject ={
		val nObject = new JSONObject()
		val types = typeString.split("\\|")
		var typeTotal = 0
		types.foreach(v =>{
			val hitCount = text.split(v).length - 1
			typeTotal += hitCount
			nObject.put(v,hitCount)
		})
		nObject.put(typeName,typeTotal)
		nObject
	}
}
