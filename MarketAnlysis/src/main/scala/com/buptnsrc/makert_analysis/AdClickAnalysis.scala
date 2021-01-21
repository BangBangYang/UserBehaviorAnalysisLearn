package com.buptnsrc.makert_analysis

import java.net.URL
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author yangkun
 * @date 2021/1/16 10:54
 * @version 1.0
 */
//定义输入输出样例类
case class AdClick(userId:Long,adId:Long,pronvice:String,city:String,timeStamp:Long)
case class AdClickCountByProvince(windowEnd:String,province:String,count:Long)
//侧输出流报警信息
case class BlackListUserWaring(userId:Long,adId:Long,msg:String)
object AdClickAnalysis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //从文件读取数据
    val rescource: URL = getClass.getResource("/AdClickLog.csv")
    val inputStream: DataStream[String] = env.readTextFile(rescource.getPath)
    //转化成样例类，并提取时间戳和watermark
    val adLogStream = inputStream
      .map(
        data => {
          val arrs: Array[String] = data.split(",")
          AdClick(arrs(0).toLong,arrs(1).toLong,arrs(2),arrs(3),arrs(4).toLong)
        }
      )
      .assignAscendingTimestamps(_.timeStamp*1000L)
    //插入一步过滤操作，并将有刷单行为的用户输出到侧输出流(黑名单报警)
    val filterBlacListUserStream:DataStream[AdClick] = adLogStream
      .keyBy(data=>(data.userId,data.adId))
      .process(new FilterBlackListUserResult(100))
    //开窗聚合统计
    val adCountResultStream = filterBlacListUserStream
      .keyBy(_.pronvice)
      .timeWindow(Time.hours(1),Time.seconds(5))
      .aggregate(new AdCountAgg(),new AdCountWindowResult())
    adCountResultStream.print("count result")
    filterBlacListUserStream.getSideOutput(new OutputTag[BlackListUserWaring]("warning")).print("warning")
    env.execute("adclick job")
  }
}
class AdCountAgg() extends AggregateFunction[AdClick,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: AdClick, accumulator: Long): Long = accumulator+1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a+b
}
class AdCountWindowResult() extends WindowFunction[Long,AdClickCountByProvince,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdClickCountByProvince]): Unit = {
    val end = new Timestamp(window.getEnd).toString
    out.collect(AdClickCountByProvince(end,key,input.head))
  }
}

class FilterBlackListUserResult(maxCount:Long) extends  KeyedProcessFunction[(Long,Long),AdClick,AdClick]{
  //定义状态，保存用户对广告点击量，每天0点定时清空状态的时间戳，标记用户是否已经进入黑名单
  lazy val countState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count",classOf[Long]))
  lazy val resetTsTimerState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("rest-ts",classOf[Long]))
  lazy val isBlackState:ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-black",classOf[Boolean]))

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClick, AdClick]#OnTimerContext, out: Collector[AdClick]): Unit = {
    if(timestamp == resetTsTimerState.value()){
      isBlackState.clear()
      countState.clear()
    }
  }

  override def processElement(value: AdClick, ctx: KeyedProcessFunction[(Long, Long), AdClick, AdClick]#Context, out: Collector[AdClick]): Unit = {
    val currentCount = countState.value()

    //判断只要是第一个数据来了，直接注册0点的状态清空定时器
    if(currentCount == 0){
      val ts = (ctx.timerService().currentProcessingTime()/(1000*60*60*24)+1)*(24*60*60*1000)-8*60*60*1000
    }
    //判断count值是否已经达到定义的阈值，如果超过就输到黑名单
    if(currentCount >= maxCount){
      //判断是否已经在黑名单里，如果没有才输出到侧输出流
      if(!isBlackState.value()){
        isBlackState.update(true)
        ctx.output(new OutputTag[BlackListUserWaring]("warning"),BlackListUserWaring(value.userId,value.adId,"Click ad over "+maxCount+" times today."))
      }
      return
    }
    //正常情况，count加1，然后数据原样输出
    countState.update(currentCount+1)
    out.collect(value)
  }
}
