package com.hugh.scala

import java.io.IOException
import java.util.Properties

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * @Author Fly.Hugh
 * @Date 2020/10/11 6:08
 * @Version 1.0
 * */
object FlinkBin {
  private val ZOOKEEPER_HOST = "172.16.10.113:2181"
  private val KAFKA_BROKER = "172.16.10.113:9092"
  private val TRANSACTION_GROUP  = "group_id_flink" //定义的消费组
  private val TOPIC = "topic.gps" //定义的topic

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers",KAFKA_BROKER)
    properties.setProperty("group.id", TRANSACTION_GROUP)

    val value: FlinkKafkaConsumer[Array[Byte]] = new FlinkKafkaConsumer(TOPIC, new ByteArrayDeserializationSchema[Array[Byte]](), properties)

    import org.apache.flink.api.scala._
    val value1: DataStream[String] = env.addSource(value).map(
      byte => {
        val string = byte.toString
        string
      }
    )

    value1.print()



    env.execute("demo")
  }

  class ByteArrayDeserializationSchema[T] extends AbstractDeserializationSchema[Array[Byte]]{
    @throws[IOException]
    override def deserialize(bytes: Array[Byte]): Array[Byte] = {
      bytes
    }
  }

}
