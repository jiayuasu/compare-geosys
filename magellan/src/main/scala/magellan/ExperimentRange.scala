package magellan

import com.vividsolutions.jts.geom.MultiPolygon
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.types._

case class PolygonRecord(polygon: Polygon)

object ExperimentRange {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
//            .setMaster("local[2]")
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

    val input = args(0)

    import sqlContext.implicits._

    // load pickup points
    val pickup_txts = spark
      .read
      .format("csv")
      .load(input)
      .toDF("x", "y")
      .toDF

    pickup_txts.createOrReplaceTempView("points")

    val pickup = spark
      .sql("select cast(pt.x as Double) as x, cast(pt.y as Double) as y from points as pt")
      .select(point($"x", $"y").as("point"))
      .index(30)

    val ring = Array(Point(-74.0, 40.7), Point(-74.0, 40.911),
      Point(-73.879, 40.911), Point(-73.879, 40.7),
      Point(-74.0, 40.7))

    // 40.7,40.911,-74.008,-73.879

    val areas = sc.parallelize(Seq(
      PolygonRecord(Polygon(Array(0), ring))
    )).toDF()

    // start range
    val rangeRes = pickup.join(areas).where($"point" within $"polygon")

    rangeRes.count()



  }



}
