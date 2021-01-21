package com.bupt.networkflow_analysis

import com.bupt.networkflow_analysis.UniqueVisitor.getClass
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 * @author yangkun
 * @date 2021/1/14 23:03
 * @version 1.0
 */
object UvWithBloom {
  def main(args: Array[String]): Unit = {
    val resources = getClass.getResource("/UserBehavior.csv")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream: DataStream[String] = env.readTextFile(resources.getPath)
    val dataStream: DataStream[UserBehavior] = inputStream.map {
      line => {
        val fields = line.split(",")
        UserBehavior(fields(0).toLong, fields(1).toLong, fields(2).toInt, fields(3), fields(4).toLong)
      }
    }.assignAscendingTimestamps(_.timestamp * 1000L)
//   dataStream.print("data")
    val uvStream = dataStream
      .filter(_.behavior == "pv")
      .map(data => ("uv",data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger()) //自定义触发器
      .process(new UvCountWithBloom())

//    uvStream.print()
    env.execute("uv with bloom job")
  }
}
// 实现自定义触发器，每个数据来了之后都触发一次窗口计算
//class MyTrigger() extends Trigger[(String, Long), TimeWindow]{
//  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE
//
//  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE
//
//  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
//
//  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE
//}

// 实现自定义的布隆过滤器
//class MyBloomFilter(size: Long) extends Serializable {
//  // 指定布隆过滤器的位图大小由外部参数指定（2的整次幂），位图存在redis中
//  private val cap = size
//
//  // 实现一个hash函数
//  def hash(value: String, seed: Int): Long = {
//    var result = 0L
//    for( i <- 0 until value.length ){
//      result = result * seed + value.charAt(i)
//    }
//    // 返回cap范围内的hash值
//    (cap - 1) & result
//  }
//}

//// 自定义ProcessFunction，实现对于每个userId的去重判断
class UvCountWithBloom1() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow]{
  // 创建redis连接和布隆过滤器
  lazy val jedis = new Jedis("10.108.113.211", 6379,10000)
  lazy val bloom = new Bloom(1 << 29)    // 1亿id，1亿个位大约需要2^27，设置大小为2^29

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    // 每来一个数据，就将它的userId进行hash运算，到redis的位图中判断是否存在
    val bitmapKey = context.window.getEnd.toString    // 以windowEnd作为位图的key

    val userId = elements.last._2.toString
    val offset = bloom.hash(userId, 61)    // 调用hash函数，计算位图中偏移量
    val isExist = jedis.getbit( bitmapKey, offset )    // 调用redis位命令，得到是否存在的结果

    // 如果存在，什么都不做；如果不存在，将对应位置1，count值加1
    // 因为窗口状态要清空，所以将count值保存到redis中
    val countMap = "uvCount"    // 所有窗口的uv count值保存成一个hash map
    val uvCountKey = bitmapKey    // 每个窗口的uv count key就是windowEnd
    var count = 0L
    // 先取出redis中的count状态
    if( jedis.hget(countMap, uvCountKey) != null )
      count = jedis.hget(countMap, uvCountKey).toLong

    if( !isExist ){
      jedis.setbit(bitmapKey, offset, true)
      // 更新窗口的uv count统计值
      jedis.hset(countMap, uvCountKey, (count + 1).toString)
      out.collect( UvCount(uvCountKey.toLong, count + 1) )
    }
  }
}
//触发器，每来一条数据，直接触发窗口计算并清空窗口状态
class MyTrigger() extends Trigger[(String,Long),TimeWindow]{
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}
//自定义布隆过滤器,主要就是一个位图和哈希函数
class Bloom(size:Long) extends Serializable{
  private val cap = size //cap应该位2的整次幂
  //哈希函数
  def hash(value:String,seed:Int): Long = {
    var result = 0
    for(i <- 0 until value.length){
      result = result*seed + value.charAt(i)
    }
    //返回hash值，要映射到cap范围内
    (cap-1) & result  //类似于取余操作，这是一个截取操作  因为cap是二的整次幂，cap-1就变成了 11111...111，然后和result进行&操作，相当于截取操作。
  }

}
//实现自定义的窗口处理函数
class UvCountWithBloom() extends ProcessWindowFunction[(String,Long),UvCount,String,TimeWindow]{// ProcessWindowFunction这是一个全窗口函数
  //定义redis连接以及布隆过滤器
  lazy val jedis = new Jedis("10.108.113.211",6379,10000)
  lazy val bloomFiler = new Bloom(1<<29)//size 就是位的个数，64MB拆成 2^6(64)* 2^20(M)* 2^3(B)= 2^29,位图中位个数约5个亿
  //本来是收集齐所有数据，窗口触发计算的时候才会调用：现在是每来一条数据都调用一次(因此自定义trigger触发)
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
      //先定义redis中存储位图的key
    val storedBitMapKey = context.window.getEnd.toString

    //另外将当前窗口的uv count值，作为状态保存到redis里边，用一个叫做uvcount的hash表来保存（windowEnd，count）
    val uvCountMap = "uvcount"
    val currengKey = context.window.getEnd.toString
    var count = 0L
    //从redis中取出当前窗口的uv count值
    if(jedis.hget(uvCountMap,currengKey)!=null) {
      count = jedis.hget(uvCountMap, currengKey).toLong
    }
      //去重：判断当前userId的hash值对应的位置的位图位置是否为0
      val userId = elements.last._2.toString
      //计算hash值，对应位图中的偏移量
      val offset = bloomFiler.hash(userId,61)
      //用redis的位操作命令，取bitmap中对应位的值
      val isExist = jedis.getbit(storedBitMapKey,offset)
      if(!isExist){
        //如果不存在，那么位图位置置1，并且将count值加1
        jedis.setbit(storedBitMapKey,offset,true)
        jedis.hset(uvCountMap,currengKey,(count+1).toString)
        out.collect( UvCount(currengKey.toLong, count + 1) )
      }

  }
}