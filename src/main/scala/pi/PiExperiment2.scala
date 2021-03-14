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


import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions.acceptPartialFunctions
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.math.random


class SequenceIterator extends java.util.Iterator[Long] with Serializable {
  var _nextId: Long = 0L

  def hasNext: Boolean = true

  def next: Long = {
    this._nextId = this._nextId + 1
    this._nextId
  }


}

class PiProcess extends KeyedProcessFunction[Long, Long, (Long, Double)] {
  override def processElement(value: Long,
                              ctx: KeyedProcessFunction[Long, Long, (Long, Double)]#Context,
                              out: Collector[(Long, Double)]):
  Unit = {
    val (x, y) = (random * 2 - 1, random * 2 - 1)
    out.collect((value, if (x * x + y * y < 1) 4.0 else 0.0))
  }

}

class AccumulatorProcess extends KeyedProcessFunction[Long, (Long, Double, Long), (Long, Double, Long)] {

  private var sum: ReducingState[java.lang.Double] = _
  private var count: ReducingState[java.lang.Long] = _

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    val sumDesc = new ReducingStateDescriptor[java.lang.Double]("sum",
      new ReduceFunction[java.lang.Double]() {
        override def reduce(t: java.lang.Double, t1: java.lang.Double): java.lang.Double = t + t1
      }, Types.DOUBLE)
    sum = getRuntimeContext.getReducingState(sumDesc)
    val countDesc = new ReducingStateDescriptor[java.lang.Long]("count",
      new ReduceFunction[java.lang.Long]() {
        override def reduce(t: java.lang.Long, t1: java.lang.Long): java.lang.Long = t + t1
      },
      Types.LONG)
    count = getRuntimeContext.getReducingState(countDesc)

  }

  //var sum: DoubleCounter = _
  //var count: LongCounter = _
  override def processElement(i: (Long, Double, Long),
                              ctx: KeyedProcessFunction[Long, (Long, Double, Long), (Long, Double, Long)]#Context,
                              out: Collector[(Long, Double, Long)]): Unit = {

    sum.add(i._2)
    count.add(i._3)
    val (s, c) = (sum.get(), count.get())
    out.collect((i._1, sum.get(), count.get()))

  }
}

/*
class PiSink extends SinkFunction[(Long, Double)] {

  override def invoke(value: (Long, Double), context: SinkFunction.Context): Unit = {
    println(s"The empirical pi is  ${value._2} after ${value._1} darts")
    context.

  }
} */
class IdGenerator extends FromIteratorFunction[Long](new SequenceIterator)


object PiExperiment2 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env
      .addSource(new IdGenerator).name("Generate IDs").keyBy(_ % 5)
      .process(new PiProcess).name("Dart Randomly")
      // .keyBy()
      .mapWith { case (id, pi) => (id, pi, 1L) }.name("Add Count")
      .keyBy(_._1 % 5)

      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .reduce { (x, y) => (1L, x._2 + y._2, x._3 + y._3) }.name("Create Mass Sum")
      .keyBy(_._1)
      .process(new AccumulatorProcess).name("Add Accumulated Results")
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .reduce { (x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3) }.name("Create Final Sum")
      .mapWith { case (_, sum, count) => (count, sum / count) }.name("Calculate Average")
      .addSink(xy => println(s"The empirical pi is  ${xy._2} after ${xy._1} darts")).name("Print Pi")
    env.execute("Pi Approximation")

  }
}
