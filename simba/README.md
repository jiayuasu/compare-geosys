## Experiment for Simba
All function calls are put in **src/main/scala/org/apache/spark/sql/simba/examples/Exmperiments.scala**

Spatial co-location pattern mining is put in **SpatialColocation.scala**.

Please pass in the parameters like range query windows, knn query point. Function call arguments are clearly marked out.


## How to submit the query

1. compile this folder `sbt clean package`
2. Take the binary jar `SimbaMaven-1.0-SNAPSHOT.jar`
3. Run `./spark-submit SimbaMaven-1.0-SNAPSHOT.jar --master spark://YOURIP:7077 queryname inputfile1 arg1 arg2 ...`

Example: `./spark-submit --class org.apache.spark.sql.simba.examples.Experiments SimbaMaven-1.0-SNAPSHOT.jar range hdfs://nyctaxi/pickuponly -74.25,40.5,-73.7,40.9 5 1 false
`

This will run a spatial range query on NYCtaxi data 5 times without using Simba R-Tree index. Use "-74.25,40.5,-73.7,40.9 5" as the query window.