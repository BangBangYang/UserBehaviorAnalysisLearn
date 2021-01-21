package com.bupt.hotitems_analysis

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.{EnvironmentSettings, Slide}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * @author yangkun
 * @date 2021/1/12 23:09
 * @version 1.0
 */
object HotItemWithSql {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //处理时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputStream: DataStream[String] = env.readTextFile("rescoures/UserBehavior.csv")
    val dataStream: DataStream[UserBehavior] = inputStream.map {
      line => {
        val fields = line.split(",")
        UserBehavior(fields(0).toLong, fields(1).toLong, fields(2).toInt, fields(3), fields(4).toLong)
      }
    }.assignAscendingTimestamps(_.timestamp * 1000L)

    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env,settings)

    //基于DataStream创建Table
    val dataTable = tableEnv.fromDataStream(dataStream,'itemId,'behavior,'timestamp.rowtime as 'ts)
    //1. table api 进行开窗聚合统计
    val aggTable = dataTable
      .filter('behavior === "pv")
      .window(Slide over 1.hours every 5.minutes on 'ts as 'sw)
      .groupBy('itemId,'sw)
      .select('itemId,'sw.end as 'windowEnd, 'itemId.count as 'cnt)
    //2 用sql去实现topN的选取
    tableEnv.createTemporaryView("aggtable",aggTable,'itemId,'windowEnd,'cnt)
    val resultTable = tableEnv.sqlQuery(
      """
        |select *
        |from(
        |select
        |  *,
        |  row_number()
        |    over(partition by windowEnd order by cnt desc)
        |    as row_num
        |from aggtable)
        |where row_num <= 5
        |
        |
        |""".stripMargin
    )
//    aggTable.toRetractStream[Row].print("res")s
    // 实现方式2：用纯sql
    tableEnv.createTemporaryView("datatable",dataStream,'itemId,'behavior,'timestamp.rowtime as 'ts)
    val resultTable1 = tableEnv.sqlQuery(
      """
        |select *
        |from(
        |select
        |  *,
        |  row_number()
        |    over(partition by windowEnd order by cnt desc)
        |    as row_num
        |from (
        |     select
        |       itemId,
        |       hop_end(ts,interval '5' minute,interval '1' hour) as windowEnd,
        |       count(itemId) as cnt
        |       from datatable
        |       where behavior = 'pv'
        |       group by
        |       itemId,
        |       hop(ts,interval '5' minute,interval '1' hour)
        |
        |))
        |where row_num <= 5
        |
        |
        |""".stripMargin
    )
    resultTable1.toRetractStream[Row].print("res")
    env.execute("sql")
  }

}
