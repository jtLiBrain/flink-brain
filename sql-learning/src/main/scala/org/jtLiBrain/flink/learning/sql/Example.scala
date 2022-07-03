package org.jtLiBrain.flink.learning.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object Example {
  /*def main(args: Array[String]): Unit = {
    // 1. create TableEnvironment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val evnSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()

    val tableEnv = StreamTableEnvironment.create(env, evnSettings)
    // val tableEnv = TableEnvironment.create(evnSettings)

    // 2. create table
//    tableEnv.fromXXX()

    val table: Table = tableEnv.from("tableName")

    // register table
    tableEnv.createTemporaryView("", table)


    // https://ci.apache.org/projects/flink/flink-docs-release-1.13/api/java/
    tableEnv.connect()
    tableEnv.executeSql("")
  }*/
}
