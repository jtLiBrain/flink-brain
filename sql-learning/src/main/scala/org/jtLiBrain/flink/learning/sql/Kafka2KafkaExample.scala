package org.jtLiBrain.flink.learning.sql

import org.apache.flink.table.api.Expressions.{$, concat, lit}
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object Kafka2KafkaExample {
  def main(args: Array[String]): Unit = {
    val evnSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
     val tableEnv = TableEnvironment.create(evnSettings)

    tableEnv.executeSql(
      """
        |CREATE TEMPORARY TABLE kafka_click (
        |  `user` STRING,
        |  `url` STRING,
        |  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'kafka_click',
        |  'properties.bootstrap.servers' = 'localhost:9092',
        |  'properties.group.id' = 'testGroup',
        |  'scan.startup.mode' = 'earliest-offset',
        |  'format' = 'csv'
        |)
        |""".stripMargin)

    tableEnv.executeSql(
      """
        |CREATE TEMPORARY TABLE kafka_click_2 (
        |  `user` STRING,
        |  `url` STRING
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'kafka_click_2',
        |  'properties.bootstrap.servers' = 'localhost:9092',
        |  'format' = 'csv'
        |)
        |""".stripMargin)

    val table1 = tableEnv.from("kafka_click").select(
      concat($("user"), lit("_2")),
      $("url")
    )

    table1.executeInsert("kafka_click_2")
  }
}
