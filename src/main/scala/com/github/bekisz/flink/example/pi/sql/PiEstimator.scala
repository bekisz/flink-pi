package com.github.bekisz.flink.example.pi.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * Runs a Pi approximation with Monte Carlo method via Flink's <B>SQL</B> API.
 *
 * Generates random points in a 2x2 box, centered in the origin.
 * The ratio of withinCircle points in the sample estimates Pi/4.
 * Rationale : The area of the 2x2 box is 4, the area of the 1 unit radius circle inside is 1*1*Pi = Pi
 * by definition. So the ratio of these two areas are :
 * Pi/4 = P(isWithinCircle)/1 => Pi = 4 * P(isWithinCircle)
 */

object PiEstimator {

  private val tableEnv = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val envSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, envSettings)
    val conf = tableEnv.getConfig.getConfiguration
    conf.setString("table.exec.mini-batch.enabled", "true") // local-global aggregation depends on mini-batch is enabled
    conf.setString("table.exec.mini-batch.allow-latency", "3s")
    conf.setString("table.exec.mini-batch.size", "1000000")
    conf.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE") // enable two-phase, i.e. local-global aggregation
    tableEnv
  }

  def main(args: Array[String]): Unit = {
    s"""
       | CREATE TEMPORARY TABLE `InputGenerator` (id BIGINT NOT NULL)
       |     WITH ('connector' = 'datagen', 'rows-per-second' = '2147483647');
       | CREATE FUNCTION PI_CORE AS 'com.github.bekisz.flink.example.pi.sql.PiEstimatorCoreFunction';
       | CREATE VIEW `PiOutput` AS SELECT PI_CORE(id) as estimatedPi FROM `InputGenerator`;
       | CREATE VIEW `PiResult` AS SELECT AVG(estimatedPi) AS estimatedPi, COUNT(*) AS trials FROM `PiOutput`;
       | CREATE TABLE `PiResultText` (text STRING) WITH('connector' = 'print');
       | INSERT INTO `PiResultText` SELECT CONCAT('The empirical PI = ', CAST(estimatedPi AS STRING), ' with ',
       |    CAST(trials/1000000 AS STRING), ' million trials ') FROM `PiResult`
    """.stripMargin.split(';').map(tableEnv.executeSql(_))
  }
}
