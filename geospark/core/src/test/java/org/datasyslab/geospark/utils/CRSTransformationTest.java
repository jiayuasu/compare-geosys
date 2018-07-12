/*
 * FILE: CRSTransformationTest
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
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
 *
 */
package org.datasyslab.geospark.utils;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.knnJudgement.GeometryDistanceComparator;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialOperator.KNNQuery;
import org.datasyslab.geospark.spatialOperator.RangeQuery;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

// TODO: Auto-generated Javadoc

/**
 * The Class CRSTransformationTest.
 */
public class CRSTransformationTest
{

    /**
     * The sc.
     */
    public static JavaSparkContext sc;

    /**
     * The prop.
     */
    static Properties prop;

    /**
     * The input.
     */
    static InputStream input;

    /**
     * The Input location.
     */
    static String InputLocation;

    /**
     * The offset.
     */
    static Integer offset;

    /**
     * The splitter.
     */
    static FileDataSplitter splitter;

    /**
     * The index type.
     */
    static IndexType indexType;

    /**
     * The num partitions.
     */
    static Integer numPartitions;

    /**
     * The query envelope.
     */
    static Envelope queryEnvelope;

    /**
     * The loop times.
     */
    static int loopTimes;

    /**
     * The query point.
     */
    static Point queryPoint;

    /**
     * The grid type.
     */
    static GridType gridType;

    /**
     * The Input location query polygon.
     */
    static String InputLocationQueryPolygon;

    /**
     * The top K.
     */
    static int topK;

    /**
     * Sets the up before class.
     *
     * @throws Exception the exception
     */
    @BeforeClass
    public static void setUpBeforeClass()
            throws Exception
    {
        SparkConf conf = new SparkConf().setAppName("PointRange").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        prop = new Properties();
        input = CRSTransformationTest.class.getClassLoader().getResourceAsStream("crs.test.properties");

        offset = 0;
        splitter = null;
        indexType = null;
        numPartitions = 0;
        GeometryFactory fact = new GeometryFactory();
        try {
            // load a properties file
            prop.load(input);
            // There is a field in the property file, you can edit your own file location there.
            // InputLocation = prop.getProperty("inputLocation");
            InputLocation = "file://" + CRSTransformationTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
            InputLocationQueryPolygon = "file://" + CRSTransformationTest.class.getClassLoader().getResource(prop.getProperty("queryPolygonSet")).getPath();
            offset = Integer.parseInt(prop.getProperty("offset"));
            splitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
            gridType = GridType.getGridType(prop.getProperty("gridType"));
            indexType = IndexType.getIndexType(prop.getProperty("indexType"));
            numPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
            queryEnvelope = new Envelope(30.01, 40.01, -90.01, -80.01);
            loopTimes = 5;
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
        finally {
            if (input != null) {
                try {
                    input.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        queryPoint = fact.createPoint(new Coordinate(34.01, -84.01));
        topK = 100;
    }

    /**
     * Tear down.
     *
     * @throws Exception the exception
     */
    @AfterClass
    public static void tearDown()
            throws Exception
    {
        sc.stop();
    }

    /**
     * Test spatial range query.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialRangeQuery()
            throws Exception
    {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, StorageLevel.MEMORY_ONLY(), "epsg:4326", "epsg:3005");
        for (int i = 0; i < loopTimes; i++) {
            long resultSize = RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, false).count();
            assert resultSize == 3127;
        }
        assert RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, false).take(10).get(1).getUserData().toString() != null;
    }

    /**
     * Test spatial range query using index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialRangeQueryUsingIndex()
            throws Exception
    {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, StorageLevel.MEMORY_ONLY(), "epsg:4326", "epsg:3005");
        spatialRDD.buildIndex(IndexType.RTREE, false);
        for (int i = 0; i < loopTimes; i++) {
            long resultSize = RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, true).count();
            assert resultSize == 3127;
        }
        assert RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, true).take(10).get(1).getUserData().toString() != null;
    }

    /**
     * Test spatial knn query.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialKnnQuery()
            throws Exception
    {
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, true, StorageLevel.MEMORY_ONLY(), "epsg:4326", "epsg:3005");

        for (int i = 0; i < loopTimes; i++) {
            List<Point> result = KNNQuery.SpatialKnnQuery(pointRDD, queryPoint, topK, false);
            assert result.size() > 0;
            assert result.get(0).getUserData().toString() != null;
            //System.out.println(result.get(0).getUserData().toString());
        }
    }

    /**
     * Test spatial knn query using index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialKnnQueryUsingIndex()
            throws Exception
    {
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, true, StorageLevel.MEMORY_ONLY(), "epsg:4326", "epsg:3005");
        pointRDD.buildIndex(IndexType.RTREE, false);
        for (int i = 0; i < loopTimes; i++) {
            List<Point> result = KNNQuery.SpatialKnnQuery(pointRDD, queryPoint, topK, true);
            assert result.size() > 0;
            assert result.get(0).getUserData().toString() != null;
            //System.out.println(result.get(0).getUserData().toString());
        }
    }

    /**
     * Test spatial KNN correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialKNNCorrectness()
            throws Exception
    {
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, true, StorageLevel.MEMORY_ONLY(), "epsg:4326", "epsg:3005");
        List<Point> resultNoIndex = KNNQuery.SpatialKnnQuery(pointRDD, queryPoint, topK, false);
        pointRDD.buildIndex(IndexType.RTREE, false);
        List<Point> resultWithIndex = KNNQuery.SpatialKnnQuery(pointRDD, queryPoint, topK, true);
        GeometryDistanceComparator geometryDistanceComparator = new GeometryDistanceComparator(this.queryPoint, true);
        List<Point> resultNoIndexModifiable = new ArrayList<>(resultNoIndex);
        List<Point> resultWithIndexModifiable = new ArrayList<>(resultWithIndex);
        Collections.sort(resultNoIndexModifiable, geometryDistanceComparator);
        Collections.sort(resultWithIndexModifiable, geometryDistanceComparator);
        int difference = 0;
        for (int i = 0; i < topK; i++) {
            if (geometryDistanceComparator.compare(resultNoIndex.get(i), resultWithIndex.get(i)) != 0) {
                difference++;
            }
        }
        assert difference == 0;
    }

    /**
     * Test spatial join query with polygon RDD.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialJoinQueryWithPolygonRDD()
            throws Exception
    {

        PolygonRDD queryRDD = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY(), "epsg:4326", "epsg:3005");

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY(), "epsg:4326", "epsg:3005");

        spatialRDD.spatialPartitioning(gridType);

        queryRDD.spatialPartitioning(spatialRDD.grids);

        List<Tuple2<Polygon, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD, queryRDD, false, true).collect();

        assert result.get(1)._1().getUserData() != null;
        for (int i = 0; i < result.size(); i++) {
            if (result.get(i)._2().size() != 0) {
                assert result.get(i)._2().iterator().next().getUserData() != null;
            }
        }
    }

    /**
     * Test spatial join query with polygon RDD using R tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialJoinQueryWithPolygonRDDUsingRTreeIndex()
            throws Exception
    {

        PolygonRDD queryRDD = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY(), "epsg:4326", "epsg:3005");

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY(), "epsg:4326", "epsg:3005");

        spatialRDD.spatialPartitioning(gridType);

        spatialRDD.buildIndex(IndexType.RTREE, true);

        queryRDD.spatialPartitioning(spatialRDD.grids);

        List<Tuple2<Polygon, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD, queryRDD, false, true).collect();

        assert result.get(1)._1().getUserData() != null;
        for (int i = 0; i < result.size(); i++) {
            if (result.get(i)._2().size() != 0) {
                assert result.get(i)._2().iterator().next().getUserData() != null;
            }
        }
    }

    /**
     * Test polygon distance join with CRS transformation.
     *
     * @throws Exception the exception
     */
    @Test
    public void testPolygonDistanceJoinWithCRSTransformation()
            throws Exception
    {
        PolygonRDD queryRDD = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY(), "epsg:4326", "epsg:3857");
        CircleRDD windowRDD = new CircleRDD(queryRDD, 0.1);
        PolygonRDD objectRDD = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY(), "epsg:4326", "epsg:3857");
        objectRDD.rawSpatialRDD.repartition(4);
        objectRDD.spatialPartitioning(GridType.RTREE);
        objectRDD.buildIndex(IndexType.RTREE, true);
        windowRDD.spatialPartitioning(objectRDD.grids);

        List<Tuple2<Geometry, HashSet<Polygon>>> results = JoinQuery.DistanceJoinQuery(objectRDD, windowRDD, true, false).collect();
        assertEquals(5467, results.size());

        for (Tuple2<Geometry, HashSet<Polygon>> tuple : results) {
            for (Polygon polygon : tuple._2()) {
                assertTrue(new Circle(tuple._1(), 0.1).covers(polygon));
            }
        }
    }
}
