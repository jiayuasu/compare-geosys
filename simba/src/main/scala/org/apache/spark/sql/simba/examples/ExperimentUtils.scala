package org.apache.spark.sql.simba.examples

import org.apache.spark.sql.{DataFrame, Dataset, functions}
import org.apache.spark.sql.simba.examples.Experiments.{PointD, PolygonD}
import org.apache.spark.sql.simba.spatial.{Point, Polygon}
import org.apache.spark.sql.simba.SimbaSession

object ExperimentUtils {

  def registerPolygonParser(simba : SimbaSession) : Unit = {
    simba.udf.register("str_to_polygon", (row : String) => {
      val pureStr = row.substring(10).dropRight(2).split(",")
      val points = pureStr.map(x => {
        val coords = x.split(" ");
        var offset = 0
        if(coords.length > 2) offset = 1
        Point(Array(coords(offset).toDouble, coords(offset+1).toDouble))
      })
      Polygon(points)
    })
  }

  def readShape(simba : SimbaSession, input : String, format : String) : DataFrame = {
      if(format.equals("polygon")){
        registerPolygonParser(simba)
        val data = simba.read.text(input)
        data.filter(functions.length(functions.col("value")) > 10).select(functions.callUDF("str_to_polygon", functions.col("value"))).toDF("polygon")
      }else{
        val data = simba.read.csv(input).toDF("x", "y")
        data.createOrReplaceTempView("points")
        simba.sql("select cast(pt.x as Double) as x, cast(pt.y as Double) as y from points as pt")
      }
  }
}
