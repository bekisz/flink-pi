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

case class PiTrialOutput(withinCircle: Boolean)
case class PiAggregation(empiricalPi: Double, count: Long) {
  override def toString: String
  = s"The empirical PI = $empiricalPi after ${count/(1*1000*1000)} million trials." +
    s"\tDistance from PI = ${Math.abs(Math.PI - empiricalPi)}"
}

object PiExperimentSQL {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val envSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, envSettings)

    val piOutput = env
      .addSource(new IdGenerator).name("Id Generator")
      .mapWith { _ =>
        val (x, y) = (random * 2 - 1, random * 2 - 1)
        PiTrialOutput(x * x + y * y < 1)
      }.name("Random Darts")

    tableEnv.createTemporaryView("PiOutputTable", piOutput)

    val query = "SELECT 4.0 * AVG(CAST(withinCircle AS Double)), COUNT(*) " +
      "FROM PiOutputTable HAVING COUNT(*) % 1000000 = 0"

    val table = tableEnv.sqlQuery(query)
    tableEnv.toRetractStream[PiAggregation](table)
      .filterWith{ case (isUpdate, _)  => isUpdate }
      .mapWith{  case (_, piAggr) => piAggr }
      .addSink(result => println(result.toString)).name("Pi Sink")
    env.execute("Pi Estimation with Flink SQL")

  }
}
