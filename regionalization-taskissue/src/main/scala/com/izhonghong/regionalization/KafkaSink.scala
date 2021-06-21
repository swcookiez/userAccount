package com.izhonghong.regionalization

import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
//                    此形参列表类型为函数
class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {
    // This is the key idea that allows us to work around running intoNotSerializableExceptions.
    lazy val producer = createProducer()  //懒加载，在使用前不会被赋值（函数不会被执行），所以KafkaProducer，在被广播时不需要被序列化。
    def send(topic: String, key: K, value: V): Future[RecordMetadata] =
      producer.send(new ProducerRecord[K, V](topic, key, value))
    def send(topic: String, value: V): Future[RecordMetadata] =
      producer.send(new ProducerRecord[K, V](topic, value))
}

object KafkaSink {
    import scala.collection.JavaConversions._
    def apply[K, V](config: Map[String, Object]): KafkaSink[K, V] = {
      val createProducerFunc = () => { //形参列表赋值
        println("produce init")
        val producer = new KafkaProducer[K, V](config)
        sys.addShutdownHook {
          producer.close()
        }
        producer
      }
      new KafkaSink(createProducerFunc)
    }
    def apply[K, V](config: java.util.Properties): KafkaSink[K, V] = apply(config.toMap)
}
