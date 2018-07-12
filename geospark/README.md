## Experiment for GeoSpark

All GeoSpark function calls are put in **/core/src/main/scala/org/datasyslab/geospark/showcase/GeoSparkExperiment**

There are around 35 queries with different GeoSpark settings (index, spatial partitoining, etc...).

Please change the parameters like range query windows, knn query point in **GeoSparkExperiment** file.

## How to submit the query

1. compile in the `core` folder: `mvn clean install -DskipTests`
2. Take the binary jar in `core/target` folder: geospark-1.1.3.jar
3. Run `./spark-submit geospark-1.1.3.jar --master spark://YOURIP:7077 queryname inputfile1 outputfile`

Example1: `./spark-submit geospark-1.1.3.jar --master spark://YOURIP:7077 nycpointrange hdfs://nyctaxi/pickuponly/* hdfs://nyctaxi/rangequeryresult`

This will run a range query on nyctaxi point type data. It uses no index.

Example2: `./spark-submit geospark-1.1.3.jar --master spark://YOURIP:7077 nycpointknn hdfs://nyctaxi/pickuponly/*`

This will run a KNN query on nyctaxi point type data. KNN query is printed to stdout directly since it is small enough. It uses no index.

Example3: `./spark-submit geospark-1.1.3.jar --master spark://YOURIP:7077 pointjoinkdb hdfs://nyctaxi/pickuponly/* hdfs://postal_codes_wkt hdfs://nyctaxi/joinqueryresult`

This will run a range join query on nyctaxi point type data and osm postal codes data. It uses KDB-Tree partitioning and no index.