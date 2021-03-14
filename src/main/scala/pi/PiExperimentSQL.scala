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
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import scala.math.random


case class PiOutput(piValue:Double)

class PiOutputIterator2 extends java.util.Iterator[PiOutput] with Serializable {
  def hasNext: Boolean = true
  def next: PiOutput = {
    val (x, y) = (random * 2 - 1, random * 2 - 1)
    PiOutput(if (x * x + y * y < 1) 4.0 else 0.0)
  }
}

object PiExperimentSQL {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val tEnv = StreamTableEnvironment.create(env)
    val fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings)


    val source = env.addSource(new PiSource).name("piSource")


    fsTableEnv.fromDataStream(source)
    fsTableEnv.createTemporaryView("piSource", source)
    println()
    // register the Table projTable as table "projectedTable"
    //tableEnv.createTemporaryView("projectedTable", projTable)
    val query = "SELECT AVG(piValue) AS avgPi FROM piSource GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND)"
    //val table = fsTableEnv.sqlQuery()

    // Just for printing purposes, in reality you would need something other than Row
    //tableEnv.toAppendStream(table, classOf[Nothing]).print


  }
}
