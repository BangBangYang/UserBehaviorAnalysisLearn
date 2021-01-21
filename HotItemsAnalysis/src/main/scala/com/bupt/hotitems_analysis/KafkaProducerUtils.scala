package com.bupt.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.BufferedSource

/**
 * @author yangkun
 * @date 2021/1/10 10:40
 * @version 1.0
 */
object KafkaProducerUtils {
  def main(args: Array[String]): Unit = {

    writeToKafka("a")
  }
  def writeToKafka(topic:String): Unit ={
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers","hdp4.buptnsrc.com:6667")
    properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String,String](properties)
    val buffersource: BufferedSource = io.Source.fromFile("rescoures/UserBehavior.csv")
    var count = 0
    for(line <- buffersource.getLines()){
      count += 1
      if(count % 1000 == 0){
        println("send "+count +" nums")
      }
      val record: ProducerRecord[String, String] = new ProducerRecord[String,String](topic,line)
      producer.send(record)
    }
    producer.close()

  }

}
