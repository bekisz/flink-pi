package com.github.bekisz.flink.example.pi.mixed


import com.github.bekisz.flink.example.pi.ds.{IdGenerator, PiAggregation}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.extensions.acceptPartialFunctions
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import scala.math.random

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
  private val env = StreamExecutionEnvironment.getExecutionEnvironment
  private val tableEnv = {
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
    val piOutput = env
      .addSource(new IdGenerator).name("Id Generator")
      .mapWith { _ =>
        val (x, y) = (random * 2 - 1, random * 2 - 1)
        x * x + y * y < 1
      }.name("Inner or Outer Circle Random Trials")

    tableEnv.createTemporaryView("PiOutput", piOutput)
    val sql = "SELECT 4.0 * AVG(CAST(f0 AS DOUBLE)), COUNT(*) FROM PiOutput"
    val piAggregationTable = tableEnv.sqlQuery(sql)
    tableEnv.toRetractStream[PiAggregation](piAggregationTable)
      .filterWith { case (isUpdate, _) => isUpdate }.mapWith { case (_, piAggr) => piAggr }
      .addSink(result => println(result.toString)).name("Pi Sink")
    env.execute("Pi Estimation with Flink SQL")
  }
}
