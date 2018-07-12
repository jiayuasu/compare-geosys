# Compare distributed spatial data management systems

This is a repository that contains a set of experiments on four state-of-art distribute spatial data management systems: GeoSpark, Simba, Magellan, and SpatialHadoop.

The purpose of these experiments is to explore the performance difference among these systems.

## Tested software

The source code of the tested system is put in corresponding folders. We also implement a couple of applications on top of them to see whether they work well in practice.

The versions are as follows:

GeoSpark 1.1.3

Simba for Spark 2.1

Magellan 1.0.6

SpatialHadoop 2.4.2


## Recommended performance metrics

* Execution time/Throughput

* Peak memory utilization

* Shuffled data (read/write)

## Recommended periformance monitoring tool

* Apache Spark web UI
* [Ganglia monitoring system](http://ganglia.sourceforge.net/)

## Tested functions

To learn the commands/scripts to run particular functions in a system, please jump to the correspondng sub-folder.

#### Spatial range query

### Spatial KNN query

#### Spatial join query (aka., range join query)

#### Range heat map

#### Spatial aggregation

#### Spatial co-location pattern mining

## Tested data

* The copyrights of the data belong to their original authors.
* Please use the original sources to download the data. 

#### Original data

|   Dataset   | Publisher                       |  Size  | Description                                                                                                                                                                                                                                                                                                                                                                       | Link                                                                                                           |
|:-----------:|---------------------------------|:------:|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
|  OSMpostal  | OpenStreetMap                   | 1.4 GB | 171 thousand polygons, all postal areas on the planet                                                                                                                                                                                                                                                                                                                             | [provided by SpatialHadoop dataset website](https://drive.google.com/file/d/0B1jY75xGiy7eNF9SWEFJeXlVSjg/view) |
|  TIGERedges | U.S. Census Bureau              |  62 GB | 72.7 million line strings, all streets in US. Each street is a line string which consists of multiple line segments                                                                                                                                                                                                                                                               | [provided by SpatialHadoop dataset website](https://drive.google.com/file/d/0B1jY75xGiy7eUW8tcGpTZnVKaTQ/view) |
|  OSMobject  | OpenStreetMap                   |  90 GB | 263 million polygons, all spatial objects on the planet (from Open Street Map)                                                                                                                                                                                                                                                                                                    | [provided by SpatialHadoop website](https://drive.google.com/file/d/0B1jY75xGiy7ecVpuTWxoRXM5VDA/view)         |
|   NYCtaxi   | NYC Taxi & Limousine Commission | 180 GB | 1.3 billion points, New York City Yellow Taxi trip information                                                                                                                                                                                                                                                                                                                    | [provided by NYC taxi company website](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)          |
| NYCtaxizone | NYC Taxi & Limousine Commission |  <1GB  | This polygon shapefile represents the boundaries zones for taxi pickups as delimited by the New York City Taxi and Limousine Commission (TLC).                                                                                                                                                                                                                                    | [provided by New York University](https://geo.nyu.edu/catalog/nyu-2451-36743)                                  |
| NYClandmark | U.S. Census Bureau              |  <1GB  | This polygon layer is a combination of the county Census TIGER landmark files for the five boroughs, re-projected to local state plane. Its features and attributes are unmodified from the original file. Features include parks, cemeteries, wildlife areas, airports, transit yards, and large public buildings like hospitals museums, colleges, and city government offices. | [provided by New York University](https://geo.nyu.edu/catalog/nyu_2451_34514)                                  |

#### Cleaned data

We put the cleaned data in our Amazona S3 bucket: [Our bucket](https://datasyslab.s3.amazonaws.com/index.html).

We also changed them to proper formats (such as WKT) which are acceptable for all tested systems.

We also provide the projected versions of the data (spatial attributes only).
 
## Contributors

* The copyrights of tested systems belong to their original authors.

* The following people collaborated together to prepare these experiments:

	- Jia Yu (Arizona State University, USA)

	- Zongsi Zhang (Arizona State University, USA, now at Grab Ltd., Singapore)

	- Mohamed Sarwat (Arizona State University, USA) 

## Discussion

We will release an article in the near future. Stay tuned!