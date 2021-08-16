package org.jtLiBrain.flink.learning.stream.connect

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ConnectBroadcastStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()

    val eventDS = env.socketTextStream("localhost", 8888)
      .map(str => str).setParallelism(3)


    val confDS = env.socketTextStream("localhost", 9999)
      .map(str => str.toInt)


    val ruleStateDescriptor = new MapStateDescriptor[String, Int](
      "RulesBroadcastState",
      classOf[String],
      classOf[Int]);

    val broadcastStream = confDS.broadcast(ruleStateDescriptor)

    eventDS.connect(broadcastStream)
      .process(new BroadcastProcessFunction[String, Int, String]{
        override def processElement(value: String, ctx: BroadcastProcessFunction[String, Int, String]#ReadOnlyContext, out: Collector[String]): Unit = {
          val iter = ctx.getBroadcastState(ruleStateDescriptor).immutableEntries().iterator()
          if(iter.hasNext) {
            while (iter.hasNext) {
              val conf = iter.next()
              out.collect(s"$value for state[key=${conf.getKey}, value=${conf.getValue}]")
            }
          } else {
            out.collect(s"$value for state[key=N/A, value=N/A}]")
          }
        }

        override def processBroadcastElement(value: Int, ctx: BroadcastProcessFunction[String, Int, String]#Context, out: Collector[String]): Unit = {
          println("processBroadcastElement():" +value)
//          ctx.getBroadcastState(ruleStateDescriptor).put("HARD_CODE_KEY", value)
//          ctx.getBroadcastState(ruleStateDescriptor).put(value+"", value)
        }
      }).print()

    env.execute()

  }
}
