package com.github.bekisz.flink.example.pi.sql

import org.apache.flink.table.functions.ScalarFunction

import scala.math.random

class PiEstimatorCoreFunction extends ScalarFunction {
  def eval(id: Long): Double = {
    val (x, y) = (random * 2 - 1, random * 2 - 1)
    if (x * x + y * y < 1) 4.0 else 0.0
  }
}

