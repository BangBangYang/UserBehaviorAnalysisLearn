package com.bupt.oderpay_detect

import java.net.URL
import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author yangkun
 * @date 2021/1/17 11:43
 * @version 1.0
 */
// 定义输入输出样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)
case class OrderPayResult(orderId: Long, resultMsg: String)
object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //从文件读取数据,转化成样例类，并提取时间戳和watermark
    val rescource: URL = getClass.getResource("/OrderLog.csv")
    val orderEventStream: DataStream[OrderEvent] = env.socketTextStream("hdp4.buptnsrc.com",9999)
//    val orderEventStream: DataStream[OrderEvent] = env.readTextFile(rescource.getPath)
      .map(
        data => {
          val arrs = data.split(",")
          OrderEvent(arrs(0).toLong,arrs(1),arrs(2),arrs(3).toLong)
        }
      )
      .assignAscendingTimestamps(_.timestamp*1000L)
      .keyBy(_.orderId)

    //1. 定义一个pattern
    val orderPattern = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    //2. 将 pattern 应用到数据流上，进行模式检测
    val patternSteam = CEP.pattern(orderEventStream,orderPattern)
    //3. 定义测输出流，用于处理超时事件
    val orderTimeoutOutputTag = new OutputTag[OrderPayResult]("orderTimeout")
    //4 调用select方法，提取并处理匹配成功的支付事件以及超时事件
    val resultStream = patternSteam.select(orderTimeoutOutputTag,new OrderTimeOutSelect(),new OrderPaySelect())
    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")
    env.execute("order timeout job")
  }
}
//实现自定义的patternTimeoutFunction以及PatternSelectFunction
class OrderTimeOutSelect() extends PatternTimeoutFunction[OrderEvent,OrderPayResult]{
  override def timeout(map: util.Map[String, util.List[OrderEvent]], timeoutTimeStamp: Long): OrderPayResult = {
    //注意这里边没有 "pay",因为这是在15min内没有匹配到的
    val timeoutOrderId = map.get("create").iterator().next().orderId
    OrderPayResult(timeoutOrderId,"timeout "+": "+timeoutTimeStamp) //

  }
}
class OrderPaySelect() extends PatternSelectFunction[OrderEvent,OrderPayResult]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderPayResult = {
    val payedOrderId = map.get("pay").iterator().next().orderId
    OrderPayResult(payedOrderId,"payed success")
  }
}
