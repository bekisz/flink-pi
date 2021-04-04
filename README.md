# flink-pi


Simple Scala examples for using [Apache Flink's](https://flink.apache.org/) DataStream 
and SQL API for the same problem, that is simple enough 
not to involve other libraries. 

The example approximates Pi with the Monte Carlo method using Flink's
* `DataStream API` : [ds.PiEstimator](https://github.com/bekisz/flink-pi/blob/main/src/main/scala/com/github/bekisz/flink/example/pi/ds/PiEstimator.scala)
* `SQL API (Blink)` : [sql.PiEstimator](https://github.com/bekisz/flink-pi/blob/main/src/main/scala/com/github/bekisz/flink/example/pi/sql/PiEstimator.scala)
* `DataStream and SQL API` : [mixed.PiEstimator](https://github.com/bekisz/flink-pi/blob/main/src/main/scala/com/github/bekisz/flink/example/pi/mixed/PiEstimator.scala)

All three examples have the same functionality, by iteratively 
approaching better and better estimate for the value of Pi. 
All write the actual best estimate in the given intervals (3 sec)  
till an explicit user interruption.  


## How it Works 

As a continuous DataSource, (random/sequnetial) IDs are generated either by 
[IdGenerator](https://github.com/bekisz/flink-pi/blob/main/src/main/scala/com/github/bekisz/flink/example/pi/ds/IdGenerator.scala) 
or by [DataGen SQL Connector](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connectors/datagen.html#datagen-sql-connector).

The next step is the creation is random point in a 2x2 box , centered in the origin, for each generated ID. 
The ratio of `withinCircle` points in the sample estimates Pi/4.
Rationale : The area of the 2x2 box is 4, the area of the 1 unit radius circle inside is `1*1*Pi = Pi`
by definition. As a consequence,  the ratio of these two areas are :
`Pi/4 = P(isWithinCircle)/1`=> `Pi = 4 * P(isWithinCircle)`
The data from the millions of trials (resulting 4.0's and 0.0's) are aggregated in two phases. The first
phase is on each thread (the bulk of the workload), while the second is a global aggregation.

The output is collected in [Print SQL Sink](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/print.html) 
or Flink's DataStreamSink.