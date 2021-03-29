/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pi

import org.apache.flink.streaming.api.scala._
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

object PiEstimatorSQL {

  private def configureTableEnv(env:StreamExecutionEnvironment) : StreamTableEnvironment = {
    val envSettings = EnvironmentSettings.newInstance()
    .useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, envSettings)
    val configuration = tableEnv.getConfig.getConfiguration
    configuration.setString("table.exec.mini-batch.enabled", "true") // local-global aggregation depends on mini-batch is enabled
    configuration.setString("table.exec.mini-batch.allow-latency", "1s")
    configuration.setString("table.exec.mini-batch.size", "1000000")
    configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE") // enable two-phase, i.e. local-global aggregation
    tableEnv
  }
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = configureTableEnv(env)
    val piOutput = env
      .addSource(new IdGenerator).name("Id Generator")
      .mapWith { _ =>
        val (x, y) = (random * 2 - 1, random * 2 - 1)
        PiTrialOutput(isWithinCircle =  x * x + y * y < 1)
      }.name("Inner or Outer Circle Random Trials")

    tableEnv.createTemporaryView("PiOutputTable", piOutput)
    val sql = "SELECT 4.0 * AVG(CAST(isWithinCircle AS Double)), COUNT(*) FROM PiOutputTable"
    val piAggregationTable = tableEnv.sqlQuery(sql)
    tableEnv.toRetractStream[PiAggregation](piAggregationTable)
      .filterWith{ case (isUpdate, _)  => isUpdate }.mapWith{  case (_, piAggr) => piAggr }
      .addSink(result => println(result.toString)).name("Pi Sink")
    env.execute("Pi Estimation with Flink SQL")
  }
}
