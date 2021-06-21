package com.izhonghong.spark.util

import com.izhonghong.spark.weibo.Weibo2IndexTable.Weibo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}


object MyESUtil {
  private val ES_HOST = "http://10.248.161.16"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = _

  /**
   * 构建客户端工厂对象
   */
  def buildFactory(): Unit = {
    val config: HttpClientConfig = new HttpClientConfig.Builder(s"$ES_HOST:$ES_HTTP_PORT")
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000)
      .readTimeout(30000)
      .build()
    factory = new JestClientFactory()
    factory.setHttpClientConfig(config)
  }

  /**
   * 批量操作测试
   *
   * @param client
   * @return
   */
  def multiOperation(client: JestClient) = {
    val source1 = Weibo("mid777", "uid222", "", "")
    val source2 = Weibo("mid888", "uid333", "", "")
    val bulkBuilder = new Bulk.Builder().defaultIndex("weibo_user_regionalization").defaultType("weibo_user_regionalizationtype")
    bulkBuilder.addAction(new Index.Builder(source1).build())
    bulkBuilder.addAction(new Index.Builder(source2).build())
    client.execute(bulkBuilder.build())
  }

  def main(args: Array[String]): Unit = {
    multiOperation(getClient())
  }

  /**
   * 获取客户端对象
   *
   * @return
   */
  def getClient(): JestClient = {
    if (factory == null) buildFactory()
    factory.getObject
  }

  /**
   * 关闭客户端
   *
   * @param client
   */
  def closeClient(client: JestClient) = {
    if (client != null) {
      try {
        client.close()
      } catch {
        case e => e.printStackTrace()
      }
    }
  }

  /**
   * 批量插入数据
   * 插入的时候保证至少传一个 source 进来
   *
   */
  def insertBulk(index: String, typeName: String, sources: Iterable[Any]) = {
    val bulkBuilder = new Bulk.Builder().defaultIndex(index).defaultType(typeName)
    sources.foreach(any => {
      bulkBuilder.addAction(new Index.Builder(any).build())
    })
    val client: JestClient = getClient()
    client.execute(bulkBuilder.build())
    closeClient(client)
  }

}