/**
 * Copyright 2015 Ram Sriharsha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package magellan

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait TestSparkContext extends BeforeAndAfterAll { self: Suite =>
  @transient var sc: SparkContext = _
  @transient var spark: SparkSession = _
  @transient var sqlContext: SQLContext = _

  override def beforeAll() {
    super.beforeAll()
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("MagellanUnitTest")
      .set("spark.sql.crossJoin.enabled", "true")

    spark = SparkSession.builder()
      .config(conf)
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()
    sqlContext = spark.sqlContext
    sc = spark.sparkContext
  }

  override def afterAll() {

    if (spark != null) {
      spark.stop()
    }
    spark = null
    sqlContext = null
    sc = null
    super.afterAll()
  }
}

