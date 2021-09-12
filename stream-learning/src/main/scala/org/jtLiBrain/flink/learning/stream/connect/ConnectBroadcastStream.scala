package org.jtLiBrain.flink.learning.stream.connect

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ConnectBroadcastStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()

  // (pid, 时间戳)
  val orderStream = env.socketTextStream("localhost", 8888)
    .map{ str =>
      val array = str.split(",")
      (array(0).toInt, array(1).toLong)
    }

  // (pid, pname)
  val dimStream = env.socketTextStream("localhost", 9999)
    .map{ str =>
      val array = str.split(",")
      (array(0).toInt, array(1))
    }

  // 1 定义广播状态描述符
  val dimStateDesc = new MapStateDescriptor[Int, String]("ProductBroadcastState",
                                                          classOf[Int],
                                                          classOf[String]);

  // 2 广播维度数据
  val dimBroadcastStream = dimStream.broadcast(dimStateDesc)

  // 3 将订单流与维度广播流进行连接
  val resultStream = orderStream.connect(dimBroadcastStream)
    .process(new BroadcastProcessFunction[(Int, Long), (Int, String), (Int, String, Long)] {
      // 3.1 处理广播流数据
      override def processBroadcastElement(value: (Int, String),
                                           ctx: BroadcastProcessFunction[(Int, Long), (Int, String), (Int, String, Long)]#Context,
                                           out: Collector[(Int, String, Long)]): Unit = {
        val dimState = ctx.getBroadcastState(dimStateDesc)

        var str = "processBroadcastElement():\n"
        val iter = ctx.getBroadcastState(dimStateDesc).immutableEntries().iterator()
        var has = false
        str += "\tExisting Broadcast State:\n"
        if (iter.hasNext) {
          has = true
          while (iter.hasNext) {
            val conf = iter.next()
            str += s"\t\tstate(pid=${conf.getKey}, pname=${conf.getValue})\n"
          }
        }

        if (!has) {
          str += "\t\t(N/A)\n"
        }

        str += s"\tNew value: $value is coming.\n\n"
        println(str)
        dimState.put(value._1, value._2)
      }

      // 3.2 处理非广播流数据
      override def processElement(value: (Int, Long),
                                  ctx: BroadcastProcessFunction[(Int, Long), (Int, String), (Int, String, Long)]#ReadOnlyContext,
                                  out: Collector[(Int, String, Long)]): Unit = {
        val dimState = ctx.getBroadcastState(dimStateDesc)
        if(dimState.contains(value._1)) {
          out.collect((value._1, dimState.get(value._1), value._2))
        } else {
          out.collect((value._1, "N/A", value._2))
        }
      }
    })

    resultStream.print()

    env.execute()

  }
}
