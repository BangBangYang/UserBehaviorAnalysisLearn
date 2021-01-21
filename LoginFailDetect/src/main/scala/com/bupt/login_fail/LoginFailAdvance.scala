package com.bupt.login_fail

import java.net.URL

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @author yangkun
 * @date 2021/1/16 16:38
 * @version 1.0
 */
/*
1.无法处理乱序数据
2.拓展性差，例如我要检测2s之内五次登陆失败，就要写很多if else
* */

object LoginFailAdvance {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //从文件读取数据
    val rescource: URL = getClass.getResource("/LoginLog.csv")
    val inputStream: DataStream[String] = env.readTextFile(rescource.getPath)
    //转化成样例类，并提取时间戳和watermark
    val loginEventStream = inputStream
      .map(
        data => {
          val arrs = data.split(",")
          LoginEvent(arrs(0).toLong,arrs(1),arrs(2),arrs(3).toLong)
        }
      )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent) = element.timestamp*1000L
      })
    //    loginEventStream.print()
    //进行判断和检测，如果2秒之内连续登陆失败，输出报警信息
    val loginFailWarningStream = loginEventStream
      .keyBy(_.userId)
        .process(new LoginFailWarningAdvanceResult())

    loginFailWarningStream.print()
    env.execute("login fail detect job")
  }

}
class LoginFailWarningAdvanceResult() extends KeyedProcessFunction[Long,LoginEvent,LoginFailWarning]{
  //定义状态，保存当前所有的登陆失败事件，保存定时器的时间戳
  lazy val loginFailListState:ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginFail-list",classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    //首先判断事件类型
    if(value.eventType == "fail") {
      //1.如果是失败，进一步做判断
      val iter = loginFailListState.get().iterator()
      //判断之前是否有登陆失败事件
      if (iter.hasNext) {
        //1.1 如果有，那么判断两次失败的时间差
        val firstFailEvent = iter.next()
        if (value.timestamp < firstFailEvent.timestamp + 2) {
          //如果两秒之内，输出报警
          out.collect(LoginFailWarning(value.userId, firstFailEvent.timestamp, value.timestamp, "login fail 2 times in 2 seconds"))
        }
        //不管报不报警，当前都处理完毕，将状态更新为最近依次登陆失败的事情
        loginFailListState.clear()
        loginFailListState.add(value)
      }
        else {
          //1.2 如果没有，直接把当前事件添加到ListState中
          loginFailListState.add(value)
        }

    }
      else {
        //2.如果是成功，直接清空状态
      loginFailListState.clear()

      }

  }
}
