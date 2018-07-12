/**
  * FILE: GeoSparkExperiment
  * PATH: org.datasyslab.geospark.showcase.GeoSparkExperiment
  * Copyright (c) GeoSpark Development Team
  *
  * MIT License
  *
  * Permission is hereby granted, free of charge, to any person obtaining a copy
  * of this software and associated documentation files (the "Software"), to deal
  * in the Software without restriction, including without limitation the rights
  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  * copies of the Software, and to permit persons to whom the Software is
  * furnished to do so, subject to the following conditions:
  *
  * The above copyright notice and this permission notice shall be included in all
  * copies or substantial portions of the Software.
  *
  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  * SOFTWARE.
  */
package org.datasyslab.geospark.showcase

import com.vividsolutions.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.{JoinQuery, KNNQuery, RangeQuery}
import org.datasyslab.geospark.spatialRDD._



object GeoSparkExperiment extends App with Logging{

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)


  var rangQueryWindow = new Envelope(-74.25,-73.7,40.5,40.9)

  var geometryFactory = new GeometryFactory()
  var queryPoint = geometryFactory.createPoint(new Coordinate(-73.973010,40.763201))
  var k = 100

  if(args(0).equalsIgnoreCase("nycpointrange"))testNYCpointRange()
  if(args(0).equalsIgnoreCase("nycpointrangertree"))testNYCpointRangeRtree()
  if(args(0).equalsIgnoreCase("nycpointrangequadtree"))testNYCpointRangeQuadtree()
  if(args(0).equalsIgnoreCase("edgesrange"))testEdgesRange()
  if(args(0).equalsIgnoreCase("edgesrangertree"))testEdgesRangeRtree()
  if(args(0).equalsIgnoreCase("edgesrangequadtree"))testEdgesRangeQuadtree()
  if(args(0).equalsIgnoreCase("osmrange"))testOsmRange()
  if(args(0).equalsIgnoreCase("osmrangertree"))testOsmRangeRtree()
  if(args(0).equalsIgnoreCase("osmrangequadtree"))testOsmRangeQuadtree()

  if(args(0).equalsIgnoreCase("nycpointknn"))testNYCpointKNN()
  if(args(0).equalsIgnoreCase("nycpointknnrtree"))testNYCpointKNNRtree()
  if(args(0).equalsIgnoreCase("edgesknn"))testEdgesKNN()
  if(args(0).equalsIgnoreCase("edgesknnrtree"))testEdgesKNNRtree()
  if(args(0).equalsIgnoreCase("osmknn"))testOsmKNN()
  if(args(0).equalsIgnoreCase("osmknnrtree"))testOsmKNNRtree()


  if(args(0).equalsIgnoreCase("pointjoinkdb"))testPointJoinKDB()
  if(args(0).equalsIgnoreCase("pointjoinquad"))testPointJoinQuadTree()
  if(args(0).equalsIgnoreCase("pointjoinr"))testPointJoinRTree()

  if(args(0).equalsIgnoreCase("pointjoinkdbindex"))testPointJoinKDBwithIndex()
  if(args(0).equalsIgnoreCase("pointjoinquadindex"))testPointJoinQuadTreewithIndex()
  if(args(0).equalsIgnoreCase("pointjoinrindex"))testPointJoinRTreewithIndex()

  if(args(0).equalsIgnoreCase("colocation"))testPointJoinKDBwithIndexColocation()

  if(args(0).equalsIgnoreCase("osmjoinkdb"))testPolygonJoinKDB()
  if(args(0).equalsIgnoreCase("osmjoinquad"))testPolygonJoinQuadTree()
  if(args(0).equalsIgnoreCase("osmjoinr"))testPolygonJoinRTree()

  if(args(0).equalsIgnoreCase("osmjoinkdbindex"))testPolygonJoinKDBwithIndex()
  if(args(0).equalsIgnoreCase("osmjoinquadindex"))testPolygonJoinQuadTreewithIndex()
  if(args(0).equalsIgnoreCase("osmjoinrindex"))testPolygonJoinRTreewithIndex()

  if(args(0).equalsIgnoreCase("edgesjoinkdb"))testEdgesJoinKDB()
  if(args(0).equalsIgnoreCase("edgesjoinquad"))testEdgesJoinQuadTree()
  if(args(0).equalsIgnoreCase("edgesjoinr"))testEdgesJoinRTree()

  if(args(0).equalsIgnoreCase("edgesjoinkdbindex"))testEdgesJoinKDBwithIndex()
  if(args(0).equalsIgnoreCase("edgesjoinquadindex"))testEdgesJoinQuadTreewithIndex()
  if(args(0).equalsIgnoreCase("edgesjoinrindex"))testEdgesJoinRTreewithIndex()

  if(args(0).equalsIgnoreCase("aggregation"))testAggregation()

//  def windowParser(input: String): Envelope =
//  {
//    // e.g.: -74.25,-73.7,40.5,40.9
//    var xyArray = input.split(",");
//    new Envelope(xyArray(0).toDouble, xyArray(1).toDouble, xyArray(2).toDouble, xyArray(3).toDouble)
//  }

  def initConf(): SparkContext =
  {
    val conf = new SparkConf().setAppName(args(0))
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    val sc = new SparkContext(conf)
    sc
  }

  def testNYCpointRange(): Unit =
  {
    val sc = initConf()
    val pointRDD = new PointRDD(sc,args(1),0,FileDataSplitter.CSV,false)
    val rangeResult = RangeQuery.SpatialRangeQuery(pointRDD, rangQueryWindow, false, false)
    rangeResult.saveAsTextFile(args(2));
    sc.stop()
  }

  def testNYCRangeIndex(indexType: IndexType): Unit =
  {
    val sc = initConf()
    val pointRDD = new PointRDD(sc,args(1),0,FileDataSplitter.CSV,false)
    pointRDD.buildIndex(indexType,false)
    logWarning(args(0)+" finished index building, index num "+pointRDD.indexedRawRDD.count())
    val rangeResult = RangeQuery.SpatialRangeQuery(pointRDD, rangQueryWindow, false, true)
    rangeResult.saveAsTextFile(args(2));
    sc.stop()
  }

  def testNYCpointRangeRtree(): Unit =
  {
    testNYCRangeIndex(IndexType.RTREE)
  }

  def testNYCpointRangeQuadtree(): Unit =
  {
    testNYCRangeIndex(IndexType.QUADTREE)
  }

  def testEdgesRange(): Unit =
  {
    val sc = initConf()
    val linestringRDD = new LineStringRDD(sc,args(1),0,1,FileDataSplitter.WKT,false)
    val rangeResult = RangeQuery.SpatialRangeQuery(linestringRDD, rangQueryWindow, false, false)
    rangeResult.saveAsTextFile(args(2))
    sc.stop()
  }

  def testEdgesRangeIndex(indexType: IndexType): Unit =
  {
    val sc = initConf()
    val linestringRDD = new LineStringRDD(sc,args(1),0,1,FileDataSplitter.WKT,false)
    linestringRDD.buildIndex(indexType,false)
    logWarning(args(0)+" finished index building, index num "+linestringRDD.indexedRawRDD.count())
    val rangeResult = RangeQuery.SpatialRangeQuery(linestringRDD, rangQueryWindow, true, true)
    rangeResult.saveAsTextFile(args(2))
    sc.stop()
  }

  def testEdgesRangeRtree(): Unit =
  {
    testEdgesRangeIndex(IndexType.RTREE)
  }

  def testEdgesRangeQuadtree(): Unit =
  {
    testEdgesRangeIndex(IndexType.QUADTREE)
  }

  def testOsmRange(): Unit =
  {
    val sc = initConf()
    val polygonRDD = new PolygonRDD(sc,args(1),0,1,FileDataSplitter.WKT,false)
    val rangeResult = RangeQuery.SpatialRangeQuery(polygonRDD, rangQueryWindow, true, false)
    rangeResult.saveAsTextFile(args(2))
    sc.stop()
  }

  def testOsmRangeIndex(indexType: IndexType): Unit =
  {
    val sc = initConf()
    val polygonRDD = new PolygonRDD(sc,args(1),0,1,FileDataSplitter.WKT,false)
    polygonRDD.buildIndex(indexType,false)
    logWarning(args(0)+" finished index building, index num "+polygonRDD.indexedRawRDD.count())
    val rangeResult = RangeQuery.SpatialRangeQuery(polygonRDD, rangQueryWindow, true, true)
    rangeResult.saveAsTextFile(args(2))
    sc.stop()
  }

  def testOsmRangeRtree(): Unit =
  {
    testOsmRangeIndex(IndexType.RTREE)
  }

  def testOsmRangeQuadtree(): Unit =
  {
    testOsmRangeIndex(IndexType.QUADTREE)
  }

  def testNYCpointKNN(): Unit =
  {
    val sc = initConf()
    val spatialRdd = new PointRDD(sc,args(1),0,FileDataSplitter.CSV,false)
    val result = KNNQuery.SpatialKnnQuery(spatialRdd, queryPoint, k, false)
    println(result)
    sc.stop()
  }

  def testNYCpointKNNRtree(): Unit =
  {
    val sc = initConf()
    val spatialRdd = new PointRDD(sc,args(1),0,FileDataSplitter.CSV,false)
    spatialRdd.buildIndex(IndexType.RTREE,false)
    logWarning(args(0)+" finished index building, index num "+spatialRdd.indexedRawRDD.count())
    val result = KNNQuery.SpatialKnnQuery(spatialRdd, queryPoint, k, true)
    println(result)
    sc.stop()
  }

  def testEdgesKNN(): Unit =
  {
    val sc = initConf()
    val spatialRdd = new LineStringRDD(sc,args(1),0,1,FileDataSplitter.WKT,false)
    val result = KNNQuery.SpatialKnnQuery(spatialRdd, queryPoint, k, false)
    println(result)
    sc.stop()
  }

  def testEdgesKNNRtree(): Unit =
  {
    val sc = initConf()
    val spatialRdd = new LineStringRDD(sc,args(1),0,1,FileDataSplitter.WKT,false)
    spatialRdd.buildIndex(IndexType.RTREE,false)
    logWarning(args(0)+" finished index building, index num "+spatialRdd.indexedRawRDD.count())
    val result = KNNQuery.SpatialKnnQuery(spatialRdd, queryPoint, k, true)
    println(result)
    sc.stop()
  }

  def testOsmKNN(): Unit =
  {
    val sc = initConf()
    val spatialRdd = new PolygonRDD(sc,args(1),0,1,FileDataSplitter.WKT,false)
    val result = KNNQuery.SpatialKnnQuery(spatialRdd, queryPoint, k, false)
    println(result)
    sc.stop()
  }

  def testOsmKNNRtree(): Unit =
  {
    val sc = initConf()
    val spatialRdd = new PolygonRDD(sc,args(1),0,1,FileDataSplitter.WKT,false)
    spatialRdd.buildIndex(IndexType.RTREE,false)
    logWarning(args(0)+" finished index building, index num "+spatialRdd.indexedRawRDD.count())
    val result = KNNQuery.SpatialKnnQuery(spatialRdd, queryPoint, k, true)
    println(result)
    sc.stop()
  }

  def testPointJoin(gridType: GridType): Unit = {
    val sc = initConf()
    val spatialRDD = new PointRDD(sc, args(1), 0, FileDataSplitter.CSV, false)
    spatialRDD.boundaryEnvelope = new Envelope(-74.25,-73.7,40.5,40.9)
    spatialRDD.approximateTotalCount = 120000000
    // revisiontest/postal_codes_wkt
    val windowRDD = new PolygonRDD(sc, args(2), 0, 1, FileDataSplitter.WKT, false)
    spatialRDD.spatialPartitioning(gridType)
    windowRDD.spatialPartitioning(spatialRDD.getPartitioner)
    val queryResult = JoinQuery.SpatialJoinQueryFlat(spatialRDD, windowRDD, false, false)
    queryResult.saveAsTextFile(args(3))
    sc.stop()
  }

  def testPointJoinKDB(): Unit = {
    testPointJoin(GridType.KDBTREE)
  }

  def testPointJoinQuadTree(): Unit = {
    testPointJoin(GridType.QUADTREE)
  }

  def testPointJoinRTree(): Unit = {
    testPointJoin(GridType.RTREE)
  }

  def testPointJoinwithIndex(gridType: GridType): Unit = {
    val sc = initConf()
    val spatialRDD = new PointRDD(sc, args(1), 0, FileDataSplitter.CSV, false)
    spatialRDD.boundaryEnvelope = new Envelope(-74.25,-73.7,40.5,40.9)
    spatialRDD.approximateTotalCount = 120000000
    // revisiontest/postal_codes_wkt
    val windowRDD = new PolygonRDD(sc, args(2), 0, 1, FileDataSplitter.WKT, false)
    spatialRDD.spatialPartitioning(gridType)
    windowRDD.spatialPartitioning(spatialRDD.getPartitioner)
    spatialRDD.buildIndex(IndexType.QUADTREE, true)
    val queryResult = JoinQuery.SpatialJoinQueryFlat(spatialRDD, windowRDD, true, false)
    queryResult.saveAsTextFile(args(3))
    sc.stop()
  }

  def testPointJoinKDBwithIndex(): Unit = {
    testPointJoinwithIndex(GridType.KDBTREE)
  }

  def testPointJoinQuadTreewithIndex(): Unit = {
    testPointJoinwithIndex(GridType.QUADTREE)
  }

  def testPointJoinRTreewithIndex(): Unit = {
    testPointJoinwithIndex(GridType.RTREE)
  }

  def testPointJoinKDBwithIndexColocation(): Unit = {
    assert(args.length == 2 || args.length == 3)
    for (i <- 1 to 2) {
      val conf = new SparkConf().setAppName(args(0))
      conf.set("spark.serializer", classOf[KryoSerializer].getName)
      conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      val sc = new SparkContext(conf)
      val spatialRDD = new PointRDD(sc, args(1), 0, FileDataSplitter.CSV, false)
      spatialRDD.boundaryEnvelope = new Envelope(4508439.377127579,4552967.173444889,-1.2617287137365943E7,-1.2395478928749988E7)
      spatialRDD.approximateTotalCount = 1221758140

      // Prepare NYC area landmarks which includes airports, museums, colleges, hospitals
      var arealmRDD = new SpatialRDD[Geometry]()
      arealmRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sc, "file:///hdd2/data/nyc-area-landmark-shapefile")
      // Use the center point of area landmarks to check co-location. This is required by Ripley's K function.
      arealmRDD.rawSpatialRDD = arealmRDD.rawSpatialRDD.rdd.map[Geometry](f=>f.getCentroid)


      arealmRDD.CRSTransform("epsg:4326","epsg:3857")
      spatialRDD.CRSTransform("epsg:4326","epsg:3857")

      arealmRDD.approximateTotalCount = 346
      //spatialRDD.analyze()
      //arealmRDD.analyze()
      logWarning("arealmRDD analyze result: boundary "+arealmRDD.boundaryEnvelope+" approximateCount: "+arealmRDD.approximateTotalCount)
      //spatialRDD.approximateTotalCount = 120000000

      if (args.length == 3) {
        spatialRDD.spatialPartitioning(GridType.KDBTREE, args(2).toInt)
      }
      else {
        spatialRDD.spatialPartitioning(GridType.KDBTREE)
      }
      spatialRDD.buildIndex(IndexType.QUADTREE, true)
      spatialRDD.indexedRDD = spatialRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY_SER)

      // Parameter settings. Check the definition of Ripley's K function.
      val area = spatialRDD.boundaryEnvelope.getArea
      val maxDistance = 0.01*Math.max(spatialRDD.boundaryEnvelope.getHeight,spatialRDD.boundaryEnvelope.getWidth)
      val iterationTimes = 10
      val distanceIncrement = maxDistance/iterationTimes
      val beginDistance = 0.0
      var currentDistance = 0.0

      // Start the iteration
      logWarning("distance(meter),observedL,difference,coLocationStatus")

      for (i <- 1 to iterationTimes)
      {
        currentDistance = beginDistance + i*distanceIncrement

        var bufferedArealmRDD = new CircleRDD(arealmRDD,currentDistance)
        bufferedArealmRDD.spatialPartitioning(spatialRDD.getPartitioner)
        //    Run GeoSpark Distance Join Query
        var adjacentMatrix = JoinQuery.DistanceJoinQueryFlat(spatialRDD, bufferedArealmRDD,true,true)

        //      Uncomment the following two lines if you want to see what the join result looks like in SparkSQL
        //      var adjacentMatrixDf = Adapter.toDf(adjacentMatrix, sparkSession)
        //      adjacentMatrixDf.show()

        var observedK = adjacentMatrix.count()*area*1.0/(arealmRDD.approximateTotalCount*spatialRDD.approximateTotalCount)
        var observedL = Math.sqrt(observedK/Math.PI)
        var expectedL = currentDistance
        var colocationDifference = observedL  - expectedL
        var colocationStatus = {if (colocationDifference>0) "Co-located" else "Dispersed"}

        logWarning(s"""$currentDistance,$observedL,$colocationDifference,$colocationStatus""")
      }

      //logWarning(args(0) + "Loop" + i + " result: " + queryResult.count())
      sc.stop()
      Thread.sleep(120000)
    }
  }

  def testPolygonJoin(gridType: GridType): Unit = {
    val sc = initConf()
    val spatialRDD = new PolygonRDD(sc,args(1),0,1,FileDataSplitter.WKT,false)
    spatialRDD.boundaryEnvelope = new Envelope(-180,180,-90,90)
    spatialRDD.approximateTotalCount = 263806686
    // revisiontest/postal_codes_wkt
    val windowRDD = new PolygonRDD(sc, args(2), 0, 1, FileDataSplitter.WKT, false)
    spatialRDD.spatialPartitioning(gridType)
    windowRDD.spatialPartitioning(spatialRDD.getPartitioner)
    val queryResult = JoinQuery.SpatialJoinQueryFlat(spatialRDD, windowRDD, false, false)
    queryResult.saveAsTextFile(args(3))
    sc.stop()
  }

  def testPolygonJoinKDB(): Unit = {
    testPolygonJoin(GridType.KDBTREE)
  }

  def testPolygonJoinQuadTree(): Unit = {
    testPolygonJoin(GridType.QUADTREE)
  }

  def testPolygonJoinRTree(): Unit = {
    testPolygonJoin(GridType.RTREE)
  }


  def testPolygonJoinwithIndex(gridType: GridType): Unit = {
    val sc = initConf()
    val spatialRDD = new PolygonRDD(sc,args(1),0,1,FileDataSplitter.WKT,false)
    spatialRDD.boundaryEnvelope = new Envelope(-180,180,-90,90)
    spatialRDD.approximateTotalCount = 263806686
    // revisiontest/postal_codes_wkt
    val windowRDD = new PolygonRDD(sc, args(2), 0, 1, FileDataSplitter.WKT, false)
    spatialRDD.spatialPartitioning(gridType)
    windowRDD.spatialPartitioning(spatialRDD.getPartitioner)
    spatialRDD.buildIndex(IndexType.QUADTREE, true)
    val queryResult = JoinQuery.SpatialJoinQueryFlat(spatialRDD, windowRDD, true, false)
    queryResult.saveAsTextFile(args(3))
    sc.stop()
  }

  def testPolygonJoinKDBwithIndex(): Unit = {
    testPolygonJoinwithIndex(GridType.KDBTREE)
  }

  def testPolygonJoinQuadTreewithIndex(): Unit = {
    testPolygonJoinwithIndex(GridType.QUADTREE)
  }

  def testPolygonJoinRTreewithIndex(): Unit = {
    testPolygonJoinwithIndex(GridType.RTREE)
  }

  def testEdgesJoin(gridType: GridType): Unit = {
    val sc = initConf()
    val spatialRDD = new LineStringRDD(sc,args(1),0,1,FileDataSplitter.WKT,false)
    spatialRDD.boundaryEnvelope = new Envelope(-126.79018,-64.630926,24.863836,50.0)
    spatialRDD.approximateTotalCount = 70380191
    // revisiontest/postal_codes_wkt
    val windowRDD = new PolygonRDD(sc, args(2), 0, 1, FileDataSplitter.WKT, false)
    spatialRDD.spatialPartitioning(gridType)
    windowRDD.spatialPartitioning(spatialRDD.getPartitioner)
    val queryResult = JoinQuery.SpatialJoinQueryFlat(spatialRDD, windowRDD, false, false)
    queryResult.saveAsTextFile(args(3))
    sc.stop()
  }

  def testEdgesJoinKDB(): Unit = {
    testEdgesJoin(GridType.KDBTREE)
  }

  def testEdgesJoinQuadTree(): Unit = {
    testEdgesJoin(GridType.QUADTREE)
  }

  def testEdgesJoinRTree(): Unit = {
    testEdgesJoin(GridType.RTREE)
  }

  def testEdgesJoinwithIndex(gridType: GridType): Unit = {
    val sc = initConf()
    val spatialRDD = new LineStringRDD(sc,args(1),0,1,FileDataSplitter.WKT,false)
    spatialRDD.boundaryEnvelope = new Envelope(-126.79018,-64.630926,24.863836,50.0)
    spatialRDD.approximateTotalCount = 70380191
    // revisiontest/postal_codes_wkt
    val windowRDD = new PolygonRDD(sc, args(2), 0, 1, FileDataSplitter.WKT, false)
    spatialRDD.spatialPartitioning(gridType)
    windowRDD.spatialPartitioning(spatialRDD.getPartitioner)
    spatialRDD.buildIndex(IndexType.QUADTREE, true)
    val queryResult = JoinQuery.SpatialJoinQueryFlat(spatialRDD, windowRDD, true, false)
    queryResult.saveAsTextFile(args(3))
    sc.stop()
  }

  def testEdgesJoinKDBwithIndex(): Unit = {
    testEdgesJoinwithIndex(GridType.KDBTREE)
  }

  def testEdgesJoinQuadTreewithIndex(): Unit = {
    testEdgesJoinwithIndex(GridType.QUADTREE)
  }

  def testEdgesJoinRTreewithIndex(): Unit = {
    testEdgesJoinwithIndex(GridType.RTREE)
  }

  def testAggregation(): Unit = {
    val sc = initConf()
    val spatialRDD = new PointRDD(sc, args(1), 0, FileDataSplitter.CSV, false)
    spatialRDD.boundaryEnvelope = new Envelope(-74.25,-73.7,40.5,40.9)
    spatialRDD.approximateTotalCount = 120000000
    val windowRDD = new SpatialRDD[Geometry]
    windowRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sc, "file:///hdd2/data/nyc_taxi_zone_shapefile")
    spatialRDD.spatialPartitioning(GridType.KDBTREE)
    windowRDD.spatialPartitioning(spatialRDD.getPartitioner)
    spatialRDD.buildIndex(IndexType.QUADTREE, true)
    val queryResult = JoinQuery.SpatialJoinQueryCountByKey(spatialRDD, windowRDD, true, false)
    queryResult.saveAsTextFile(args(3))
    sc.stop()
  }
}