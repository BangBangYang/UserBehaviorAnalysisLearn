package com.bupt.networkflow_analysis

import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * @author yangkun
 * @date 2021/1/14 18:03
 * @version 1.0
 */
//定义输入样例类
case class UserBehavior(userId: Long, itemId: Long, category: Int, behavior: String, timestamp: Long)

//定义输出样例类
case class PvCount(windowEnd: Long, count: Long)

object PageView {
  def main(args: Array[String]): Unit = {

    val resources = getClass.getResource("/UserBehavior.csv")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream: DataStream[String] = env.readTextFile(resources.getPath)
    val dataStream: DataStream[UserBehavior] = inputStream.map {
      line => {
        val fields = line.split(",")
        UserBehavior(fields(0).toLong, fields(1).toLong, fields(2).toInt, fields(3), fields(4).toLong)
      }
    }.assignAscendingTimestamps(_.timestamp * 1000L)

    val pvStream = dataStream
      .filter(_.behavior == "pv")
      //      .map(data => ("pv",1L)) //定义一个pv字符作为分组的dummy key
      .map(new MyMapper)
      .keyBy(_._1) //所有数据后被分到一个组
      .timeWindow(Time.hours(1)) //一个小时的滚动窗口
      .aggregate(new PvCountAgg(), new PvCountResult())
    //    pvStream.print() //pvStream 是每个分区的聚合结果
    val totalPvStream: DataStream[PvCount] = pvStream
      .keyBy(_.windowEnd)
      .process(new TotalPvCountResult())
    //      .sum("count")
    totalPvStream.print()
    env.execute("pageview count")
  }
}

//自定义预聚合函数
class PvCountAgg() extends AggregateFunction[(String, Long), Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//自定义窗口函数
class PvCountResult() extends WindowFunction[Long, PvCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    out.collect(PvCount(window.getEnd, input.head))
  }
}

//自定义mapper，随机生成分组的key
class MyMapper() extends MapFunction[UserBehavior, (String, Long)] {
  override def map(value: UserBehavior): (String, Long) = {
    (Random.nextString(10), 1L)
  }
}
// =============== 并行度优化 ================
// 实现自定义的KeyedProcessFunction，实现所有分组数据的聚合叠加
class TotalPvCountResult() extends KeyedProcessFunction[Long, PvCount, PvCount] { //key windowEnd 是Long类型
  // 定义一个状态，保存当前所有key的count总和
  lazy val currentTotalCountState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("total-count",classOf[Long]))


  override def processElement(value: PvCount, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#Context, out: Collector[PvCount]): Unit = {
        // 获取当前count总和
        val currentTotalCount = currentTotalCountState.value()
        // 叠加当前数据的count值，更新状态
        currentTotalCountState.update(currentTotalCount+value.count)
    // 注册定时器，1ms之后触发
        ctx.timerService().registerEventTimeTimer(value.windowEnd+1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {
    // 所有key的count值都已聚合，直接输出结果
    out.collect(PvCount(ctx.getCurrentKey,currentTotalCountState.value()))
    // 清空状态
    currentTotalCountState.clear()
  }
}