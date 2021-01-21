package com.bupt.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @author yangkun
 * @date 2021/1/13 17:12
 * @version 1.0
 */
// 输入及窗口聚合结果样例类
case class ApacheLogEvent(ip: String, userId: String, timestamp: Long, method: String, url: String)

case class PageViewCount(url: String, count: Long, windowEnd: Long)

object HotPages {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //    val inputStream: DataStream[String] = env.readTextFile("rescoures/apache.log")
    val inputStream = env.socketTextStream("hdp4.buptnsrc.com", 9999)
    val dataStream: DataStream[ApacheLogEvent] = inputStream
      .map(data => {
        val arr = data.split(" ")
        //对事件事件进行转化
        val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val ts: Long = simpleDateFormat.parse(arr(3)).getTime
        ApacheLogEvent(arr(0), arr(1), ts, arr(5), arr(6))
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) { //乱序程度一分钟
      override def extractTimestamp(element: ApacheLogEvent) = element.timestamp
    })

    //进行开窗聚合以及排序输出
    val aggStream = dataStream
      .filter(_.method == "GET")
      .keyBy(_.url) // 注意_.url这是函数，这样经过keyby后 key是跟url 一个类型（String）。要是利用“url” 分组，key的类型就是tuple类型
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(new OutputTag[ApacheLogEvent]("late"))
      .aggregate(new PageCountAgg(), new PageViewCountWindowResult())
    val resultStream = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNHotpages(3))
    dataStream.print("data")
    aggStream.print("agg")
    aggStream.getSideOutput(new OutputTag[ApacheLogEvent]("late")).print("late")
    resultStream.print("res")
    env.execute("hot pages")

  }

}

class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] { // in acc out
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class PageViewCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect(PageViewCount(key, input.iterator.next(), window.getEnd))
  }
}

class TopNHotpages(n: Int) extends KeyedProcessFunction[Long, PageViewCount, String] { //key的类型 由于keyby传参是_.windowEnd,因此windowEnd是什么类型key就是什么类型
//   lazy val pageViewCountListState:ListState[PageViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("pageViewCount-list",classOf[PageViewCount]))
  //2. 有迟到数据
  lazy val pageViewCountMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pageViewCount-map", classOf[String], classOf[Long]))

  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
//        pageViewCountListState.add(value)
    ///2. 有迟到数据
    pageViewCountMapState.put(value.url, value.count)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    //2. 有迟到数据，另外注册一个定时器，一分钟之后触发，这时窗口已经彻底关闭，不再有聚合结果输出，可以清空状态
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 60000L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
//1 .
    //       val allPageViewCounts:ListBuffer[PageViewCount] = ListBuffer()
//        val iter = pageViewCountListState.get().iterator()
//        while (iter.hasNext){
//          allPageViewCounts.append(iter.next())
//        }
    // 1. 对应没有迟到数据  提前清空状态  没有迟到数据，可以这么写
//        pageViewCountListState.clear()
    //==============================================
    //2. 有迟到数据
    //判断触发器触发时间，如果已经是窗口结束时间1分钟之后，那么直接清空状态
    val allPageViewCounts: ListBuffer[(String, Long)] = ListBuffer()
    if (timestamp == ctx.getCurrentKey + 60000L) { //key 是windowEnd
      pageViewCountMapState.clear()
      return
    }
    val iter = pageViewCountMapState.entries().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      allPageViewCounts += ((entry.getKey, entry.getValue))
    }
    // ==============================================
    //按照访问量排序top n
//        val sortedPageViewCounts = allPageViewCounts.sortWith(_.count > _.count).take(n)
    val sortedPageViewCounts = allPageViewCounts.sortWith(_._2 > _._2).take(n)
    //将排名信息格式化String,便于打印输出可视化展示
    val result: StringBuilder = new StringBuilder
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")

    //遍历结果列表中的每个ItemViewCount,输出到一行
    for (i <- sortedPageViewCounts.indices) {
      val currentItemViewCount = sortedPageViewCounts(i)
      result.append("NO").append(i + 1).append(": \t")
//                .append("页面URL=").append(currentItemViewCount.url).append("\t")
//                .append("热门度=").append(currentItemViewCount.count).append("\n")
        //2
        .append("页面URL=").append(currentItemViewCount._1).append("\t")
        .append("热门度=").append(currentItemViewCount._2).append("\n")



    }
    result.append("=============================\n\n")
    Thread.sleep(1000)

    out.collect(result.toString())

  }
}