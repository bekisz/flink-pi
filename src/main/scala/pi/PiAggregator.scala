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

import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor

@SerialVersionUID(1L)
class PiAggregator extends  ProcessFunction[Double, Double] {



  private var sum:ValueState[java.lang.Double] = _
  private var count:ValueState[java.lang.Long] = _

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    val sumDesc = new ValueStateDescriptor("sum", Types.DOUBLE)
    sum = getRuntimeContext.getState(sumDesc)
    val countDesc = new ValueStateDescriptor("count", Types.LONG)
    count = getRuntimeContext.getState(countDesc)

  }
  override def processElement(i: Double, context: ProcessFunction[Double, Double]#Context,
                              collector: Collector[Double]): Unit = {

    val countSoFar = count.value
    count.update(countSoFar + 1)
    val sumSoFar = sum.value
    sum.update(sumSoFar + i)
    if (countSoFar % 10000 ==0 ) {
      collector.collect(sum.value()/count.value())
    }
  }
}
