package com.bupt.oderpay_detect

import java.net.URL

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, KeyedCoProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author yangkun
 * @date 2021/1/17 17:14
 * @version 1.0
 */
//定义到账事件样例类
case class ReceiptEvent(txId: String, payChannel: String, timestamp: Long)

object TxMatch {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //1.从文件读取订单数据,转化成样例类，并提取时间戳和watermark
    var rescource: URL = getClass.getResource("/OrderLog.csv")

    val orderEventStream: DataStream[OrderEvent] = env.readTextFile(rescource.getPath)
      .map(
        data => {
          val arrs = data.split(",")
          OrderEvent(arrs(0).toLong, arrs(1), arrs(2), arrs(3).toLong)
        }
      )
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.eventType == "pay")
      .keyBy(_.txId)
    //2.从文件读取到账数据,转化成样例类，并提取时间戳和watermark
    rescource = getClass.getResource("/ReceiptLog.csv")

    val receiptEventStream: DataStream[ReceiptEvent] = env.readTextFile(rescource.getPath)
      .map(
        data => {
          val arrs = data.split(",")
          ReceiptEvent(arrs(0), arrs(1), arrs(2).toLong)
        }
      )
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.txId)
    //3. 合并两条流，进行处理
    val resultStream = orderEventStream.connect(receiptEventStream)
      .process(new TxPayMatchResult())
    resultStream.print("matched")
    resultStream.getSideOutput(new OutputTag[OrderEvent]("unmatched-pay")).print("unmatched-pays")
    resultStream.getSideOutput(new OutputTag[ReceiptEvent]("unmatched-receipt")).print("unmatched-receipts")
    env.execute("tx match job")
  }
}

class TxPayMatchResult() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
  //定义状态，保存当前交易对应的订单支付事件和到账事件
  lazy val payEventState:ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay",classOf[OrderEvent]))
  lazy val receiptEventState:ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt",classOf[ReceiptEvent]))
 //侧输出流标签
  val unMatchedPayedOutputTag = new OutputTag[OrderEvent]("unmatched-pay")
  val unMatchedReceiptedOutputTag = new OutputTag[ReceiptEvent]("unmatched-receipt")
  override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //订单支付事件，要判断之前是否有到账事件
    val receipt = receiptEventState.value()
    if(receipt!=null){
      //如果有receipt，正常输出匹配，清空状态
      out.collect((pay,receipt))
      receiptEventState.clear()
      payEventState.clear()
    }else{
      //如果还没来，注册定时器,等待5s
      ctx.timerService().registerEventTimeTimer(pay.timestamp*1000L+5000L)
      //更新状态
      payEventState.update(pay)
    }
  }

  override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //到账事件，要判断之前是否有订单支付事件
    val pay = payEventState.value()
    if(pay!=null){
      //如果有pay，正常输出匹配，清空状态
      out.collect((pay,receipt))
      receiptEventState.clear()
      payEventState.clear()
    }else{
      //如果还没来，注册定时器,等待3s
      ctx.timerService().registerEventTimeTimer(receipt.timestamp*1000L+3000L)
      //更新状态
      receiptEventState.update(receipt)
    }
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //定时器触发，判断状态中哪个存在，就代表另一个没来，输出到侧输出流

    if(payEventState.value() != null){
      ctx.output(unMatchedPayedOutputTag,payEventState.value())
    }
    if(receiptEventState.value()!=null){
      ctx.output(unMatchedReceiptedOutputTag,receiptEventState.value())
    }
    //清空状态
    receiptEventState.clear()
    payEventState.clear()
  }
}
