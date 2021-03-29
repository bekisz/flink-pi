package com.github.bekisz.flink.example.pi

case class PiAggregation(empiricalPi: Double, count: Long) {
  override def toString: String
  = s"The empirical PI = $empiricalPi after ${count / (1 * 1000 * 1000)} million trials." +
    s"\tDistance from real PI = ${Math.abs(Math.PI - empiricalPi)}"
}
