package org.jtLiBrain.flink.learning.stream.distributed_cache

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object UseDistributedCache {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()

    env.registerCachedFile("file:///Users/Dream/Dev/git/flink-brain/stream-learning/product.txt", "dim.txt")

    // (pid, 时间戳)
    val orderStream = env.socketTextStream("localhost", 8888)
    .map{ str =>
      val array = str.split(",")
      (array(0).toInt, array(1).toLong)
    }

    val resultStream = orderStream.flatMap(new RichFlatMapFunction[(Int, Long), (Int, String, Long)] {
      var dim: Map[Int, String] = Map()

      override def open(parameters: Configuration): Unit = {
        val dimFile = getRuntimeContext.getDistributedCache.getFile("dim.txt")
        val dimIter = FileUtils.readLines(dimFile, "utf8").iterator()
        while(dimIter.hasNext) {
          val dimIterm = dimIter.next()
          val dimArray = dimIterm.split(",")
          dim += dimArray(0).toInt -> dimArray(1)
        }
      }

      override def flatMap(in: (Int, Long), out: Collector[(Int, String, Long)]): Unit = {
        if(dim.contains(in._1)) {
          out.collect((in._1, dim.get(in._1).get, in._2))
        } else {
          out.collect((in._1, "N/A", in._2))
        }
      }
    })

    resultStream.print()

    env.execute()
  }
}
