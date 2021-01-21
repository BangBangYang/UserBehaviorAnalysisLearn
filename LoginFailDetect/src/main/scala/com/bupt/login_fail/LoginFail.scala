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
//输入的登陆事件样例类
case class LoginEvent(userId:Long,ip:String,eventType:String,timestamp:Long)
//输出报警信息样例类
case class LoginFailWarning(userId:Long,firstFailTime:Long,lastFailTime:Long,msg:String)
object LoginFail {
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
      .process(new LoginFailWarningResult(2))
    loginFailWarningStream.print()
    env.execute("login fail detect job")
  }

}
class LoginFailWarningResult(failTimes: Int) extends KeyedProcessFunction[Long,LoginEvent,LoginFailWarning]{
  //定义状态，保存当前所有的登陆失败事件，保存定时器的时间戳
  lazy val loginFailListState:ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginFail-list",classOf[LoginEvent]))
  lazy val timeTsState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time-ts",classOf[Long]))
  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    //判断当前登陆事件是成功还是失败
    if(value.eventType == "fail"){
      loginFailListState.add(value)
      //如果没有定时器，那么注册一个2s后的定时器
      if(timeTsState.value() == 0){
        val ts = value.timestamp*1000L+5000L
        ctx.timerService().registerEventTimeTimer(ts)
        timeTsState.update(ts)
      }
    }
    else{
      //如果成功，那么直接清空状态和定时器，重新开始
      ctx.timerService().deleteEventTimeTimer(timeTsState.value())
      loginFailListState.clear()
      timeTsState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit = {
    val allLoginFaiList:ListBuffer[LoginEvent] = ListBuffer()
    val iter = loginFailListState.get().iterator()
    while(iter.hasNext){
      allLoginFaiList += iter.next()
    }
    //判断登陆事件的个数，如果超过上限，报警
//    print(allLoginFaiList.length,failTimes)
    if(allLoginFaiList.length >= failTimes){

      out.collect(LoginFailWarning(allLoginFaiList.head.userId,allLoginFaiList.head.timestamp,allLoginFaiList.last.timestamp,"login fail in 2s for "+allLoginFaiList.length+" times"))

    }
    //清空状态
    loginFailListState.clear()
    timeTsState.clear()
  }
}