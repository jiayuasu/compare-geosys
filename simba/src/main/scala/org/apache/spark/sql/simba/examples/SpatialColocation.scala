package org.apache.spark.sql.simba.examples

import com.vividsolutions.jts.io.WKTReader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql
import org.apache.spark.sql.functions
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.examples.Experiments.PointD
import org.apache.spark.sql.simba.index.RTreeType

object SpatialColocation {
  def main(args: Array[String]): Unit = {
    val simba = SimbaSession
      .builder()
//            .master("local[4]")
      .appName("SparkSessionForSimba")
//      .config("simba.join.partitions", "2000")
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryoserializer.buffer.max", "1024")
      //      .config("spark.kryo.registrator", classOf[ShapeSerializer].getName)
      .config("simba.join.distanceJoin", "DJSpark")
      .config("simba.index.partitions", "64")
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    import simba.implicits._

    val input = Array(args(0), args(1))

    val area_points = polygonToPoint(simba, input(0)).as[PointD]

    val points = ExperimentUtils.readShape(simba, input(1), "point").as[PointD]

    points.createOrReplaceTempView("a")

    simba.indexTable("a", RTreeType, "quadtrees",  Array("x", "y") )

    import simba.simbaImplicits._

    points.cache()

    // Distance should be calculated from the actual bounding box of the target region
    //0.01 (1%) can be any percent.
    val maxDistance = 0.01*Math.max((40.9 - 40.5), (74.25 - 73.7))
    val iterationTimes = 10
    val distanceIncrement = maxDistance/iterationTimes
    val beginDistance = 0.0
    var currentDistance = 0.0

    for(i <- 1 to iterationTimes){

      val distance = beginDistance + distanceIncrement * i
      import simba.implicits._
      val joinDs = area_points.distanceJoin(points, Array("x", "y"), Array("x", "y"), distance)//.show( )

      val adjacentMatrixCount = joinDs.count()
      val pointsCount = points.count()
      val arealmCount = area_points.count()

      // Simba is not able to directly calculate the area of a given column. So this area is obtained by GeoSpark and handwritten here.
      // Degree-based calculation since Simba doesn't support CRS transformation.
      val area = (40.9 - 40.5) * (74.25 - 73.7)

      val observedK = adjacentMatrixCount * area * 1.0 / (arealmCount*pointsCount)
      val observedL = Math.sqrt(observedK/Math.PI)
      val expectedL = currentDistance
      var colocationDifference = observedL  - expectedL
      var colocationStatus = {if (colocationDifference>0) "Co-located" else "Dispersed"}
    }


  }

  private def polygonToPoint(simba : SimbaSession, input : String) : sql.DataFrame = {

    @transient lazy val reader = new WKTReader()
    import simba.implicits._
    val toCenter = functions.udf((str : String) => {
      val center = reader.read(str).getCentroid
      Array(center.getX, center.getY)
    })

    val wkts = simba.read.text(input).toDF("wkt").filter(functions.length($"wkt") > 10)

    val centers = wkts
      .select(toCenter($"wkt"))
      .withColumn("x", $"UDF(wkt)".getItem(0))
        .withColumn("y", $"UDF(wkt)".getItem(1))
        .drop("UDF(wkt)")
    centers
    // avg length is 0.06246827299524497
  }
}
