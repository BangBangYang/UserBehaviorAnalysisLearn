package com.bupt.hotitems_analysis

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @author yangkun
 * @date 2020/12/28 21:10
 * @version 1.0
 */
//定义输入样例类
case class UserBehavior(userId: Long, itemId: Long, category: Int, behavior: String, timestamp: Long)

//定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //处理时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //从文件中读取并转化为样例类,提取时间戳生成watermark
//    val inputStream = env.readTextFile("rescoures/UserBehavior.csv")

    //从kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hdp4.buptnsrc.com:6667")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("a", new SimpleStringSchema(), properties))
    val dataStream: DataStream[UserBehavior] = inputStream.map {
      line => {
        val fields = line.split(",")
        UserBehavior(fields(0).toLong, fields(1).toLong, fields(2).toInt, fields(3), fields(4).toLong)
      }
    }.assignAscendingTimestamps(_.timestamp * 1000L)
    //得到窗口聚合结果
    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv") //过滤pv行为
      .keyBy("itemId") //
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new ItemVievWindowResult())

    val resultStream = aggStream
      .keyBy("windowEnd") //按照窗口分组，收集当前窗口窗口内商品count的数据
      .process(new TopNHotItems(5)) //自定义处理流程
    resultStream.print()


    env.execute("hot items")


  }

}

//自定义预聚合函数AggregateFunction,聚合状态就是当前的count值
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  //每来一条数据调用一次add,count加1
  override def createAccumulator(): Long = 0

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//自定义窗口函数windowFunction
class ItemVievWindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

//自定义keyedProcessFunction
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
  //先定义状态:liststate
  private var itemViewCountListState: ListState[ItemViewCount] = _


  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //每来一条数据，直接加入listState
    itemViewCountListState.add(value)
    //注册一个windowEnd+1 之后的定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1) //定时器是根据当前的时间戳判别的，当前的windowEnd都是一样的，所以不会重复注册定时器
  }

  override def open(parameters: Configuration): Unit = {
    itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViemCount-list", classOf[ItemViewCount]))
  }

  //当定时器触发，可以认为所有窗口统计结果都已经到齐，可以排序输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //为了方便排序,另外定义一个ListBuffer,保存listState里边的所有数据
    val allItemViemCounts: ListBuffer[ItemViewCount] = ListBuffer()
    val iter = itemViewCountListState.get().iterator()
    while (iter.hasNext) {
      allItemViemCounts += iter.next()
    }
    //清空状态
    itemViewCountListState.clear()
    //按照count大小排序
    val sortedItemViemCounts = allItemViemCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize) //柯里化  倒序排序
    //将排名信息格式化String,便于打印输出可视化展示
    val result:StringBuilder = new StringBuilder
    result.append("窗口结束时间：").append(new Timestamp(timestamp-1)).append("\n")

    //遍历结果列表中的每个ItemViewCount,输出到一行
    for( i <- sortedItemViemCounts.indices){
      val currentItemViewCount = sortedItemViemCounts(i)
      result.append("NO").append(i+1).append(": \t")
        .append("商品ID=").append(currentItemViewCount.itemId).append("\t")
        .append("热门度=").append(currentItemViewCount.count).append("\n")

    }
    result.append("=============================\n\n")
    Thread.sleep(1000)

    out.collect(result.toString())
  }
}
