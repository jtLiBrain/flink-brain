package org.jtLiBrain.flink.learning.stream.join

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object IntervalJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()

    val ws = WatermarkStrategy
      .forBoundedOutOfOrderness[(String, Long)](Duration.ofSeconds(60))
      .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
        override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = element._2
      })

    val ds1 = env.socketTextStream("localhost", 8888)
      .map(str => {
        val strArray = str.split(",")
        (strArray(0), strArray(1).toLong)
      }).assignTimestampsAndWatermarks(ws)

    val ds2 = env.socketTextStream("localhost", 9999)
      .map(str => {
        val strArray = str.split(",")
        (strArray(0), strArray(1).toLong)
      }).assignTimestampsAndWatermarks(ws)

    ds1.keyBy(t => t._1)
      .intervalJoin(ds2.keyBy(t => t._1))
      .between(Time.seconds(-2), Time.seconds(3))
      .process(new ProcessJoinFunction[(String, Long), (String, Long), String] {
        override def processElement(left: (String, Long), right: (String, Long), ctx: ProcessJoinFunction[(String, Long), (String, Long), String]#Context, out: Collector[String]): Unit = {
          out.collect("LEFT:" + left + " => RIGHT:" + right)
        }
      }).print()

    env.execute()
  }
}
