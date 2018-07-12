/*
 * FILE: VectorOverlayOperator
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
package org.datasyslab.geosparkviz.core;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

// TODO: Auto-generated Javadoc

/**
 * The Class VectorOverlayOperator.
 */
public class VectorOverlayOperator
{

    /**
     * The back vector image.
     */
    public List<String> backVectorImage = null;

    /**
     * The distributed back vector image.
     */
    public JavaPairRDD<Integer, String> distributedBackVectorImage = null;

    /**
     * The generate distributed image.
     */
    public boolean generateDistributedImage = false;

    /**
     * The Constant logger.
     */
    final static Logger logger = Logger.getLogger(VectorOverlayOperator.class);

    /**
     * Instantiates a new vector overlay operator.
     *
     * @param distributedBackImage the distributed back image
     */
    public VectorOverlayOperator(JavaPairRDD<Integer, String> distributedBackImage)
    {
        this.distributedBackVectorImage = distributedBackImage;
        this.generateDistributedImage = true;
    }

    /**
     * Instantiates a new vector overlay operator.
     *
     * @param backVectorImage the back vector image
     */
    public VectorOverlayOperator(List<String> backVectorImage)
    {
        this.backVectorImage = backVectorImage;
        this.generateDistributedImage = false;
    }

    /**
     * Join image.
     *
     * @param distributedFontImage the distributed font image
     * @return true, if successful
     * @throws Exception the exception
     */
    public boolean JoinImage(JavaPairRDD<Integer, String> distributedFontImage)
            throws Exception
    {
        logger.info("[GeoSparkViz][JoinImage][Start]");
        if (this.generateDistributedImage == false) {
            throw new Exception("[OverlayOperator][JoinImage] The back image is not distributed. Please don't use distributed format.");
        }
        // Prune SVG header and footer because we only need one header and footer per SVG even if we merge two SVG images.
        JavaPairRDD<Integer, String> distributedFontImageNoHeaderFooter = distributedFontImage.filter(new Function<Tuple2<Integer, String>, Boolean>()
        {

            @Override
            public Boolean call(Tuple2<Integer, String> vectorObject)
                    throws Exception
            {
                // Check whether the vectorObject's key is 1. 1 means this object is SVG body.
                // 0 means this object is SVG header, 2 means this object is SVG footer.
                if (vectorObject._1 != 1) {
                    return false;
                }
                return true;
            }
        });
        this.distributedBackVectorImage = this.distributedBackVectorImage.union(distributedFontImageNoHeaderFooter);
        this.distributedBackVectorImage = this.distributedBackVectorImage.sortByKey();
        logger.info("[GeoSparkViz][JoinImage][Stop]");
        return true;
    }

    /**
     * Join image.
     *
     * @param frontVectorImage the front vector image
     * @return true, if successful
     * @throws Exception the exception
     */
    public boolean JoinImage(List<String> frontVectorImage)
            throws Exception
    {
        logger.info("[GeoSparkViz][JoinImage][Start]");
        if (this.generateDistributedImage == true) {
            throw new Exception("[OverlayOperator][JoinImage] The back image is distributed. Please don't use centralized format.");
        }
        // Merge two SVG images. Skip the first element and last element because they are SVG image header and footer.
        List<String> copyOf = new ArrayList<String>();
        for (int i = 0; i < this.backVectorImage.size() - 1; i++) {
            copyOf.add(this.backVectorImage.get(i));
        }
        for (int i = 1; i < frontVectorImage.size() - 1; i++) {
            copyOf.add(frontVectorImage.get(i));
        }
        copyOf.add(this.backVectorImage.get(this.backVectorImage.size() - 1));
        this.backVectorImage = copyOf;
        logger.info("[GeoSparkViz][JoinImage][Stop]");
        return true;
    }
}
