## Experiment for SpatialHadoop


## How to set up the environment
1. Compile this folder: `mvn assembly:assembly -DskipTests`
2. Change the jar in Target folder `spatialhadoop-2.4.3-SNAPSHOT.jar` to `spatialhadoop-2.4.2.jar`
3. Copy the jar to Hadoop runtime folder `hadoop-2.6.5/share/hadoop/common/lib/` on every machine of your cluster
4. Copy the content in `/src/main/package` to Hadoop runtime folder.

## How to run queries

1. Range query: `./hadoop-2.6.5/bin/shadoop rangequery hdfs://osmobjects hdfs://rangqueryresult shape:wkt rect:-90,-45,90,45 -overwrite`
This will run a range query on osmobjects with rect:-90,-45,90,45 query window.
2. KNN query:`./hadoop-2.6.5/bin/shadoop knn edges hdfs://knnqueryresult k:10 point:-73.973010,40.763201 shape:wkt -overwrite`
This will run a range query on edges with point:-73.973010,40.763201. k=10.
3. Join query: For Join, we first run index on larger dataset and then run *distributed join* that shadoop2 recommands

Index: `shadoop index nyc-wkt nyc-quadtree sindex:quadtree shape:wkt`

distributed join: `./hadoop-2.6.5/bin/shadoop dj hdfs://nycarea-landmark hdfs://nyctaxi/pickuonly myjoinquery repartition:auto direct-join:no shape:wkt -overwrite`
This will run a join query between NYClandmark dataset and NYCtaxi dataset.
4. Run heatmap: `shadoop hplot hdfs://NYCtaxi/pickuponly myheatmap titlewidth:256 titlewidth:256 levels:6 shape:wkt -pyramid -keep-ratio -vflip  -overwrite
`

Spatial aggregation code is at: `src/main/java/org/datasyslab/spatialbenchmark/SpatialAggregation.java`
