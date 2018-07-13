package magellan

import com.vividsolutions.jts.geom.MultiPolygon
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.types._

object ExperimentJoin {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      //      .setMaster("local[2]")
      .setAppName("Geospark_Magellan_test")
      .set("spark.sql.crossJoin.enabled", "true")

    val spark = SparkSession.builder()
      .config(conf)
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()
    val sqlContext = spark.sqlContext
    val sc = spark.sparkContext

    // inject rules for spatial join
    magellan.Utils.injectRules(spark)

    val inputs = Array(args(0), args(1))

    import sqlContext.implicits._

    // load pickup points
    val pickup_txts = spark
      .read
      .format("csv")
      .load(args(1))
      .toDF("x", "y")
      .toDF

    pickup_txts.createOrReplaceTempView("points")

    val pickup = spark
      .sql("select cast(pt.x as Double) as x, cast(pt.y as Double) as y from points as pt")
      .select(point($"x", $"y").as("point"))
      .index(30)


    // load taxi_areas
    @transient lazy val wktReader = new WKTReader()
    val wktToShape = functions.udf((str : String) => {
      val geom = wktReader.read(str)
      import collection.immutable.ListMap
      if(geom.isInstanceOf[com.vividsolutions.jts.geom.Polygon]){
        val polygon = geom.asInstanceOf[com.vividsolutions.jts.geom.Polygon]
        val points = polygon.getCoordinates.map(coord => {
          Point(coord.x, coord.y)
        }).toArray
        val indices = Array(0, polygon.getCoordinates.length)
        Polygon(indices, points)
      }else{
        val multiPolygon = geom.asInstanceOf[com.vividsolutions.jts.geom.MultiPolygon]
        val indexes = 0 to (multiPolygon.getNumGeometries - 1) toArray
        val polygons = indexes
          .map(i => multiPolygon.getGeometryN(i).asInstanceOf[com.vividsolutions.jts.geom.Polygon])
        val points = polygons.flatMap(polygon => {
          polygon
            .getCoordinates
            .map(coord => Point(coord.x, coord.y))
        })
        val indices = 0 to multiPolygon.getNumGeometries toArray
        var offset = 0
        for(i <- 1 to multiPolygon.getNumGeometries){
          offset = offset + polygons(i-1).getNumPoints
          indices(i) = offset
        }
        Polygon(indices, points)
      }
    })

    val areas = spark.read.text(inputs(0))
      .toDF("wkt")
      .select(wktToShape($"wkt").as("polygon"))

    // start join and statistics
    val joinRes = pickup.join(areas).where($"point" within $"polygon")

    // count
    joinRes.count()



  }



}
