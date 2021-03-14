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
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import scala.math.random


object PiExperiment3 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env
      //.generateSequence(1,10000000)
      .addSource(new IdGenerator).name("Id Generator")
      //.keyBy(_ % 1000)
      .mapWith{id =>
        val (x, y) = (random * 2 - 1, random * 2 - 1)
        ( 1L, if (x * x + y * y < 1) 1L else 0L, id)
      }
      .keyBy(_._3 %100)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
      .reduce{(x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3)}.name("Thread Local Creator")
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .reduce{(x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3)}.name("Sum Creator")
      .keyBy(_._1 % 1)
      .mapWithState{(in: (Long, Long, Long), count: Option[(Long, Long)]) =>
        count match {
          case Some(c) => ( (c._1 + in._1, c._2 + in._2), Some((c._1 + in._1, c._2+in._2 ) ))
          case None => ( (in._1, in._2), Some(in._1,in._2) )
        }}.global
      .mapWith{ case(count, sum) => (count, sum*4.0 / count)}.name("Average Calculator")
      //.print()
      .addSink( xy => println(s"The empirical pi is  ${xy._2} after ${xy._1 / 1000000L} million darts" +
        s" and this close to real PI : ${Math.abs(Math.PI-xy._2)}"))
      .name("Pi Sink")
    env.execute("Pi Approximation 3")
  }
}
