package com.bupt.networkflow_analysis

import java.lang

import com.bupt.networkflow_analysis.PageView.getClass
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author yangkun
 * @date 2021/1/14 22:20
 * @version 1.0
 */
//定义输出样例类
case class UvCount(windowEnd: Long, count: Long)
object UniqueVisitor {
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
      .timeWindowAll(Time.hours(1))
      .apply(new UvCountResult())

    pvStream.print()
    env.execute("uv job")
  }
}
// 实现自定义的WindowFunction
class UvCountResult() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    // 用一个set集合类型，来保存所有的userId，自动去重
    var idSet = Set[Long]()
    for( userBehavior <- input )
      idSet += userBehavior.userId

    // 输出set的大小
    out.collect( UvCount(window.getEnd, idSet.size) )
  }
}