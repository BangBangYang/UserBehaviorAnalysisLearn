package com.bupt.login_fail

import java.net.URL
import java.util

import com.bupt.login_fail.LoginFailAdvance.getClass
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author yangkun
 * @date 2021/1/17 9:50
 * @version 1.0
 */
object LoginFailWithCep {
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
          LoginEvent(arrs(0).toLong, arrs(1), arrs(2), arrs(3).toLong)
        }
      )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent) = element.timestamp * 1000L
      })

    //1. 定义匹配的模式，要求是一个登陆失败事件后，另一个紧跟登陆失败事件
    val loginFailPattern = Pattern
      .begin[LoginEvent]("firstFail").where(_.eventType == "fail")
      .next("secondFail").where(_.eventType == "fail")
      .next("thirdFail").where(_.eventType == "fail")
      .within(Time.seconds(5))

    //2. 将模式应用到数据流上，得到一个PatternStream
    val patternStream = CEP.pattern(loginEventStream.keyBy(_.userId),loginFailPattern)

    //3. 检出符合模式的数据流，调用select
    val loginFailWarningStream = patternStream.select(new LoginFailEvenMatch())
    loginFailWarningStream.print()
    env.execute("login fail with cep job")
  }
}
//实现自定义PatternSelectFunction
class LoginFailEvenMatch() extends PatternSelectFunction[LoginEvent,LoginFailWarning]{
  override def select(map: util.Map[String, util.List[LoginEvent]]): LoginFailWarning = {
    //匹配到的事件序列，就保存到Map里
    val firstFailEvent = map.get("firstFail").get(0)
    val thirdFailEvent= map.get("thirdFail").iterator().next()
    LoginFailWarning(firstFailEvent.userId,firstFailEvent.timestamp,thirdFailEvent.timestamp,"login fail")

  }
}