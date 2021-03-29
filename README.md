# flink-pi


Simple example for using Apache Flink's DataStream 
and SQL API for the same problem, that is simple enough 
not to involve other libraries. 

The example approximates Pi with the Monte Carlo method 
* via Flink's `DataStream API` : [PiEstimatorDS](https://github.com/bekisz/flink-pi/blob/main/src/main/scala/com/github/bekisz/flink/example/pi/PiEstimatorDS.scala)
* via Flink's `SQL API` : [PiEstimatorSQL](https://github.com/bekisz/flink-pi/blob/main/src/main/scala/com/github/bekisz/flink/example/pi/PiEstimatorSQL.scala)

Both example has the same functionality, by iteratively 
approaching better and better estimate for the value of Pi. 

##How it Works 

[IdGenerator](https://github.com/bekisz/flink-pi/blob/main/src/main/scala/com/github/bekisz/flink/example/pi/IdGenerator.scala) 
acts as custom Flink DataSource continuously generating a sequence of `Long` values from `0` to `Infinity`.
For each `id`s  in both SQL and DS versions generate random points in a 2x2 box, centered in the origin.
The ratio of `withinCircle` points in the sample estimates Pi/4.
Rationale : The area of the 2x2 box is 4, the area of the 1 unit radius circle inside is `1*1*Pi = Pi`
by definition. So the ratio of these two areas are :
`Pi/4 = P(isWithinCircle)/1`=> `Pi = 4 * P(isWithinCircle)`

