package com.bupt.oderpay_detect

import java.net.URL

import akka.event.Logging.LogEvent
import com.bupt.oderpay_detect.OrderTimeout.getClass
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author yangkun
 * @date 2021/1/17 15:29
 * @version 1.0
 */
object OrderTimeoutWithoutCep {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //从文件读取数据,转化成样例类，并提取时间戳和watermark
    val rescource: URL = getClass.getResource("/OrderLog.csv")
//    val orderEventStream: DataStream[OrderEvent] = env.socketTextStream("hdp4.buptnsrc.com",9999)
          val orderEventStream: DataStream[OrderEvent] = env.readTextFile(rescource.getPath)
      .map(
        data => {
          val arrs = data.split(",")
          OrderEvent(arrs(0).toLong,arrs(1),arrs(2),arrs(3).toLong)
        }
      )
      .assignAscendingTimestamps(_.timestamp*1000L)
      .keyBy(_.orderId)

    //自定义processFunction进行复杂事件检测
//    val orderResultStream=
    val orderResultStream: DataStream[OrderPayResult] = orderEventStream
      .keyBy(_.orderId)
      .process(new OrderPayMatchResult())



    orderResultStream.print("payed")
    orderResultStream.getSideOutput(new OutputTag[OrderPayResult]("timeout")).print("timeout")
    env.execute("order timeout without cep job")
  }
}
//自定义实现KeyedProcessFunction
class OrderPayMatchResult() extends KeyedProcessFunction[Long,OrderEvent,OrderPayResult]{
  //定义状态，标志位表示create，pay是否已经来过，定时器时间戳
  lazy val isCreatedState:ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-created",classOf[Boolean]))
  lazy val isPayedState:ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-payed",classOf[Boolean]))
  lazy val timerTsState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts",classOf[Long]))
  //定义侧输出流标签
  val orderTimeoutOutputTag = new OutputTag[OrderPayResult]("timeout")



  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderPayResult]#Context, out: Collector[OrderPayResult]): Unit = {
      //先拿到当前状态
    val isPayed = isPayedState.value()
    val isCreated = isCreatedState.value()
    val timerTs = timerTsState.value()

    //判断当前事件类型，看是create还是pay
    //1.来的是create，看是否pay过
    if(value.eventType == "create"){
      //1.1 如果已经支付过，正常支付，输出匹配成功结果
      if(isPayed){
        out.collect(OrderPayResult(value.orderId,"payed successfully"))
        //已经处理完毕，清空状态和定时器
        isCreatedState.clear()
        isPayedState.clear()
        timerTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      }else{
        //1.2 如果还没pay过，注册定时器等待15min
        val ts = value.timestamp *1000L + 900*1000L
        ctx.timerService().registerEventTimeTimer(ts)
        //更新状态
        timerTsState.update(ts)
        isCreatedState.update(true)
      }
    }
      //如果当前的是pay，判断是否created过
    else if(value.eventType == "pay"){
      if(isCreated){
        //2.1 如果已经create过，匹配成功，还要判读一下pay是否已经过了定时器的时间
        if(value.timestamp *1000L < timerTs) {
          //2.1.1没有超时，正常输出
          out.collect(OrderPayResult(value.orderId, "payed successfully"))
        }
        else{
         //2.2.2已经超时，输出到侧输出流
          ctx.output(orderTimeoutOutputTag,OrderPayResult(value.orderId,"payed but already timeout"))
        }
        //只要输出结果，当前order已经结束，清空状态和定时器
        isCreatedState.clear()
        isPayedState.clear()
        timerTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)

      }
      else {
        //2.2如果create没来，注册定时器，等到pay的时间就可以
        ctx.timerService().registerEventTimeTimer(value.timestamp*1000L)
        //更新状态
        timerTsState.update(value.timestamp*1000L)
        isPayedState.update(true)

      }
    }

  }
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderPayResult]#OnTimerContext, out: Collector[OrderPayResult]): Unit = {
    //定时器触发
    //1. pay来了，没等到create
    if(isPayedState.value()){
      ctx.output(orderTimeoutOutputTag,OrderPayResult(ctx.getCurrentKey,"payed but not found create log"))
    }else{
      //2 create 来了，没有pay
      ctx.output(orderTimeoutOutputTag,OrderPayResult(ctx.getCurrentKey,"order timeout"))
    }
    //清空状态
    isCreatedState.clear()
    isPayedState.clear()
    timerTsState.clear()
  }
}

