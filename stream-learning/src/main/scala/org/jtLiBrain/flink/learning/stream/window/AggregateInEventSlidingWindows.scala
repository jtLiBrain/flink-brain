package org.jtLiBrain.flink.learning.stream.window

import java.text.SimpleDateFormat
import java.time.Duration
import java.util.Date

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable
//
/**
 * https://stackoverflow.com/questions/65559601/apache-flink-does-not-return-data-for-idle-partitions
 * 如果所有的分区都是空闲的，那么withIdleness() 是不能提升水印的
 */
object AggregateInEventSlidingWindows {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//      .setBufferTimeout(500)
    //    env.enableCheckpointing()
//    env.setRuntimeMode(RuntimeExecutionMode.BATCH)
    //val text = env.readTextFile("file:///Users/Dream/Dev/git/flink-brain/stream-learning/data")
    val text = env.socketTextStream("localhost", 8888).setParallelism(1)

    /*val source = KafkaSource.builder[String]
      .setBootstrapServers("localhost:9092")
      .setTopics("test1")
      .setGroupId("my-group-1")
      .setStartingOffsets(OffsetsInitializer.latest)
      .setValueOnlyDeserializer(new SimpleStringSchema)
      .build

    val text = env.fromSource(source, WatermarkStrategy.noWatermarks[String], "Kafka Source")*/

    val wm = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(30))
      .withTimestampAssigner(new SerializableTimestampAssigner[Log] {
        override def extractTimestamp(element: Log, recordTimestamp: Long): Long = element.ts
      })
      .withIdleness(Duration.ofSeconds(30))

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val counts = text.map { l =>
      val arrs = l.split(",")
      val name = arrs(0)
      val ts = arrs(1)
      sdf.parse(ts).getTime
      Log(sdf.parse(ts).getTime, name, ts)
    }.assignTimestampsAndWatermarks(wm)
      .keyBy(_.name)
      .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(3)))
      .allowedLateness(Time.seconds(5))
      .aggregate(
        new AggregateFunction[Log, mutable.Buffer[String], List[String]] {
          override def createAccumulator(): mutable.Buffer[String] = {
            mutable.Buffer.empty[String]
          }

          override def add(value: Log, accumulator: mutable.Buffer[String]): mutable.Buffer[String] = {
            accumulator += value.time
          }

          override def getResult(accumulator: mutable.Buffer[String]): List[String] = {
            accumulator.toList
          }

          override def merge(a: mutable.Buffer[String], b: mutable.Buffer[String]): mutable.Buffer[String] = {
            a ++= b
          }
        },

        new ProcessWindowFunction[List[String], (String, Int, List[String]), String, TimeWindow] {
          override def process(key: String, context: Context, elements: Iterable[List[String]], out: Collector[(String,Int, List[String])]): Unit = {
            val acc = elements.iterator.next()
            val start = sdf.format(new Date(context.window.getStart))
            val end = sdf.format(new Date(context.window.getEnd))
            out.collect((s"$key\t[$start,$end)", acc.size, acc))
          }
        }
      )

    counts.print()
    env.execute("Aggregate In Event Sliding Windows")
  }
}
