## Experiment for Magellan

Spatial aggregation example is available: `src/main/scala/magellan/Application.scala`

## How to submit the query
1. Compile this folder: `sbt "set test in assembly := {}" clean assembly
`
2. Run the query: `./bin/spark-submit --class magallen.Application magellan-assembly-1.0.6-SNAPSHOT.jar hdfs://nyczones hdfs://nyctaxi/pickuponly`