package org.jtLiBrain.flink.learning.stream.join

import java.sql.{Connection, Statement}

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

class DimFlatMapFunction extends RichFlatMapFunction[(Int, String), (Int, String, String)] {
  val LOG = LoggerFactory.getLogger(classOf[DimFlatMapFunction])

  var dim: Map[Int, String] = Map()
  var conn: Connection = _
  var stat: Statement = _

  override def open(parameters: Configuration): Unit = {
    /*// initialize the connection
    conn = ... ...

    val sql = "select pid, pname from dim_product"

    try {
      stat = conn.createStatement()

      val resultSet = stat.executeQuery(sql)
      while (resultSet.next()) {
        val pId = resultSet.getInt("pid")
        val pName = resultSet.getString("pname")

        dim += pId -> pName
      }
    } catch {
      case e: Exception => LOG.error(null, e)
    }

    // close stat and conn
    ... ...*/
  }

  override def flatMap(in: (Int, String), collector: Collector[(Int, String, String)]): Unit = {
    val pidInProbe = in._1
    if(dim.contains(pidInProbe)) { // implement as Inner-Join
      collector.collect((in._1, in._2, dim.get(pidInProbe).get))
    }
  }
}
