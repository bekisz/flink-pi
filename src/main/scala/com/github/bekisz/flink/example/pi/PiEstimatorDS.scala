package com.github.bekisz.flink.example.pi

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.extensions.acceptPartialFunctions
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import scala.math.random

/**
 * Runs a Pi approximation with Monte Carlo method via Flink's <B>DataStream API</B>.
 *
 * Generates random points in a 2x2 box, centered in the origin.
 * The ratio of withinCircle points in the sample estimates Pi/4.
 * Rationale : The area of the 2x2 box is 4, the area of the 1 unit radius circle inside is 1*1*Pi = Pi
 * by definition. So the ratio of these two areas are :
 * Pi/4 = P(isWithinCircle)/1 => Pi = 4 * P(isWithinCircle)
 */

object PiEstimatorDS {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env
      .addSource(new IdGenerator).name("Id Generator")
      .mapWith { id =>
        val (x, y) = (random * 2 - 1, random * 2 - 1)
        (1L, if (x * x + y * y < 1) 1L else 0L, id)
      }.name("Random Darts")
      .keyBy(_._3 % 1000)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
      .reduce { (x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3) } // First phase of aggregation (/thread)
      .name("Thread Local Sum")
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .reduce { (x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3) } // Second phase of aggregation (global)
      .name("Global Sum")
      .keyBy(_._1 % 1)
      .mapWithState { (in: (Long, Long, Long), count: Option[(Long, Long)]) =>
        count match {
          case Some(c) => ((c._1 + in._1, c._2 + in._2), Some((c._1 + in._1, c._2 + in._2)))
          case None => ((in._1, in._2), Some(in._1, in._2))
        }
      }.name("Aggregation of Current Window with State")
      .global
      .mapWith { case (count, sum) => PiAggregation(4.0 * sum / count, count) }.name("Average Calculator")
      .addSink(result => println(result.toString)).name("Pi Sink")
    env.execute("Pi Estimator with DataStream API")
  }
}
