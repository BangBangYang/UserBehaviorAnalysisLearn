package com.buptnsrc.makert_analysis

import java.lang
import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * @author yangkun
 * @date 2021/1/15 15:41
 * @version 1.0
 */
//定义输入数据样例类
case class MarketUserBehavior(UserId: String, behavior: String, channel: String, timeStamp: Long)
//定义输出数据样例类
case class MarkertViewCount(windowStart:String,windowEnd:String,channel:String,behavior:String,count:Long)
//自定义测试数据源
class SimulatedSource extends RichSourceFunction[MarketUserBehavior] {
  //是否运行的标志位
  var running = true
  //定义用户行为和渠道的集合
  val behaviorSet: Seq[String] = Seq("view", "download", "install", "uninstall")
  val channelSet: Seq[String] = Seq("appstore", "weibo", "wechat", "tieba")
  val rand: Random = Random

  override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
    //定义一个生成数据最大数量
    val maxCounts = Long.MaxValue
    var count = 0L

    //while循环，不停产生随机数据
    while (running && count < maxCounts) {
      val id = UUID.randomUUID().toString
      val behavior = behaviorSet(rand.nextInt(behaviorSet.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()
      ctx.collect(MarketUserBehavior(id, behavior, channel, ts))
      count += 1
      Thread.sleep(50L)
    }

  }

  override def cancel(): Unit = running = false
}

//
object AppMarketByChannel {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.addSource(new SimulatedSource)
      .assignAscendingTimestamps(_.timeStamp)
    //开窗统计输出
    val resultStream=dataStream
      .filter(_.behavior != "uninstall")
//      .keyBy("channel", "behavior") //返回的是javaTuple
        .keyBy(data =>(data.channel,data.behavior)) //返回的是（String，String）
        .timeWindow(Time.days(1),Time.seconds(5))
//        .aggregate()
        .process(new MarketCountByChannel())
    resultStream.print()
    env.execute("")
  }

}
//自定义ProcessWindowFunction  全窗口函数
class MarketCountByChannel() extends ProcessWindowFunction[MarketUserBehavior,MarkertViewCount,(String,String),TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarkertViewCount]): Unit = {
    val start = new Timestamp(context.window.getStart).toString
    val end = new Timestamp(context.window.getEnd).toString

    val channel = key._1
    val behavior = key._2
    val count = elements.size
    out.collect(MarkertViewCount(start,end,channel,behavior,count))
  }
}
