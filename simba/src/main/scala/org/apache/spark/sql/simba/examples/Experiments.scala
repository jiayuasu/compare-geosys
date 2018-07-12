package org.apache.spark.sql.simba.examples

import com.vividsolutions.jts.algorithm.MinimumBoundingCircle
import com.vividsolutions.jts.io.WKTReader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Row, RowFactory, functions}
import org.apache.spark.sql.simba.{ShapeSerializer, SimbaSession}
import org.apache.spark.sql.simba.index.{RTreeType, TreapType}
import org.apache.spark.sql.simba.spatial.{Point, Polygon}
import org.apache.spark.storage.StorageLevel

object Experiments {

  case class PointD(x : Double, y : Double)
  case class PolygonD(polygon : Polygon)

//  var optType : String = ""
//  var input : Array[String] = null
//  var loop : Int = 1
//  var withIndex : Boolean = false
//  var shapetypes : Array[String] = null
//  var distance : Double = 0.001

  def main(args: Array[String]): Unit = {
    val simbaSession = SimbaSession
      .builder()
//      .master("local[4]")
      .appName("SparkSessionForSimba")
      .config("simba.join.partitions", "2000")
      .config("spark.serializer",classOf[KryoSerializer].getName)
        .config("spark.kryoserializer.buffer.max", "1024")
//      .config("spark.kryo.registrator", classOf[ShapeSerializer].getName)
        .config("simba.join.distanceJoin", "DJSpark")
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val optType = args(0)
//    val argIt = args.iterator
//    optType = argIt.next()
//    if(optType.equals("join")){
//      input = Array(argIt.next(), argIt.next())
//    }else input = Array(argIt.next())
//    shapetypes = Array(argIt.next())
//    if(optType.equals("join")) shapetypes :+ argIt.next()
//    loop = argIt.next().toInt
//    withIndex = argIt.next().toBoolean
//    if(optType.equals("join")) distance = argIt.next().toDouble
    optType match {
      case "join" => runJoin(simbaSession, args)
      case "trans" => polygonToPoint(simbaSession, args)
      case "range" => runRange(simbaSession, args)
    }
    simbaSession.stop()
  }

  private def runKNN(simba : SimbaSession, args : Array[String]): Unit = {

    // parse args
    val input = args(1)
    val k = args(2).toInt

    import simba.implicits._
    var data = simba.read.csv(input).toDF("x", "y")
    data.createOrReplaceTempView("points")
    data = simba.sql("select cast(pt.x as Double) as x, cast(pt.y as Double) as y from points as pt")
    val ds = data.as[PointD]
    ds.printSchema()
    import simba.simbaImplicits._
    println(ds.knn(Array("x", "y"),Array(1.0, 1.0),1).count)
//    if(withIndex){
//      data.createOrReplaceTempView("pts")
//      simba.indexTable("index", RTreeType, "RtreeForData",  Array("x", "y") )
//      val res = simba.sql("SELECT * FROM index")
//      for(a <- 1 to loop){
//        res.knn(Array("x", "y"), Array(1.0, 1.0), 6).count
//      }
//    }else{
//      for(a <- 1 to loop){
//        import simba.simbaImplicits._
//        println(data.knn(Array("x", "y"),Array(1.0, 1.0),1).count)
//      }
//    }
  }

  private def runRange(simba : SimbaSession, args : Array[String]) : Unit = {

    // parse args
    val input = args(1)
    val window = args(2).split(",").map(x => x.toDouble).toList
    val loop = args(3).toInt
    val scaleRatio = args(4).toInt
    val withIndex = args(5).toBoolean

    // calculate final query window
    val xAdd = (window(2) - window(0)) / scaleRatio
    val yAdd = (window(3) - window(1)) / scaleRatio
    val loPoint = Array(window(0), window(1))
    val hiPoint = Array(loPoint(0) + xAdd, loPoint(1) + yAdd)

//    println(loPoint.mkString(",") + "   " + hiPoint.mkString(","))

    import simba.implicits._
    var data = simba.read.csv(input).toDF("x", "y")
    data.createOrReplaceTempView("points")
    val ds = simba.sql("select cast(pt.x as Double) as x, cast(pt.y as Double) as y from points as pt").as[PointD]
//      .map((row : Row) =>{
//      Point(Array(row.getDouble(0), row.getDouble(1)))
//    })
    import simba.simbaImplicits._
    if(withIndex) ds.index(RTreeType, "indexForNYC",  Array("x", "y"))
    for(a <- 1 to loop){
      println(ds.range(Array("x", "y"),loPoint,hiPoint).count)
    }
  }

  private def runJoin(simba : SimbaSession, args : Array[String]) : Unit = {

    // parse args
    val input = Array(args(1), args(2))
    val distance = args(3).toDouble
    val loop = args(4).toInt

    import simba.implicits._
    import simba.simbaImplicits._
    val left = ExperimentUtils.readShape(simba, input(0), "point").as[PointD]
//      .index(RTreeType, "RtreeForLeft", Array("x", "y"))
//    val left = Seq(PointD(74.0, 40.6), PointD(-73.8, 40.7)).toDS()
    val right = ExperimentUtils.readShape(simba, input(1), "point").as[PointD]
//      .index(RTreeType, "RtreeForRight", Array("x", "y"))
//      .index(RTreeType, "RtreeForRight", Array("x", "y"))
    //left.distanceJoin(right, "polygon", Array("x", "y"), 3).show()
//    left.persist(StorageLevel.MEMORY_ONLY)
//    right.persist(StorageLevel.MEMORY_ONLY)
    for(a <- 1 to loop){
        left.distanceJoin(right, Array("x", "y"), Array("x", "y"), distance).show( )
    }
  }

  private def polygonToPoint(simba : SimbaSession, args : Array[String]) : Unit = {

    val input = args(1)

    @transient lazy val reader = new WKTReader()
    import simba.implicits._
    val toCenter = functions.udf((str : String) => {
      val center = reader.read(str).getCentroid
      center.getX.toString + "," + center.getY.toString
    })
//    val rs = functions.udf((str : String) => {
//      val mbc = new MinimumBoundingCircle(reader.read(str))
//      mbc.getRadius
//    })
    val wkts = simba.read.text(input).toDF("wkt").filter(functions.length($"wkt") > 10)
//    wkts.select(rs($"wkt")).toDF("r").select(functions.avg("r")).show()

    val centers = wkts.select(toCenter($"wkt"))
    centers.write.text("/home/jinxuanw/zzsworkspace/revisiondata/output/res")

    // avg length is 0.06246827299524497
  }
}
