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

import org.apache.flink.streaming.api.functions.source.FromIteratorFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions.acceptPartialFunctions
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import scala.math.random


class PiOutputIterator extends java.util.Iterator[Double] with Serializable {
  def hasNext: Boolean = true
  def next: Double = {
    //println(s" thread name : ${Thread.currentThread().getName}")
    val (x, y) = (random * 2 - 1, random * 2 - 1)
    if (x * x + y * y < 1) 4.0 else 0.0
  }
}
class PiSource extends FromIteratorFunction[Double](new PiOutputIterator)

object PiExperiment {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env
      .generateSequence(1,10000000)
      //.addSource(new IdGenerator).name("Id Generator")
      .mapWith{ id =>
          val (x, y) = (random * 2 - 1, random * 2 - 1)
          ( id, if (x * x + y * y < 1) 4.0 else 0.0)
      }.name("Darts")
      .keyBy(_._1 % 10)
      .mapWithState{(in: (Long, Double), count: Option[(Long, Double)]) =>
        count match {
          case Some(c) => ( (c._1 + 1L, c._2 + in._2), Some((c._1 + 1L, c._2+in._2 ) ))
          case None => ( (1L, in._2), Some(1L,in._2) )
        }}

      .keyBy(_._1 % 10)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .maxBy(0)
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(3)))
      .reduce{(x, y) => (x._1 + y._1, x._2 + y._2)}.name("Sum Creator")
      .mapWith{ case(count, sum) => (count, sum / count)}.name("Average Calculator")
      .print()
      //.addSink( xy => println(s"The empirical pi is  ${xy._2} after ${xy._1} darts")).name("Pi Sink")
    env.execute("Pi Approximation")
  }
}
