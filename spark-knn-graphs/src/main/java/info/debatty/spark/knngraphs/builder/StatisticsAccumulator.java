/*
 * The MIT License
 *
 * Copyright 2016 Thibault Debatty.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package info.debatty.spark.knngraphs.builder;

import info.debatty.java.graphs.StatisticsContainer;
import org.apache.spark.AccumulatorParam;

/**
 *
 * @author Thibault Debatty
 */
public class StatisticsAccumulator implements AccumulatorParam<StatisticsContainer> {

    public StatisticsContainer addAccumulator(StatisticsContainer arg0, StatisticsContainer arg1) {
        arg0.incAddSimilarities(arg1.getAddSimilarities());
        arg0.incRemoveSimilarities(arg1.getRemoveSimilarities());
        arg0.incSearchCrossPartitionRestarts(arg1.getSearchCrossPartitionRestarts());
        arg0.incSearchRestarts(arg1.getSearchRestarts());
        arg0.incSearchSimilarities(arg1.getSimilarities());
        return arg0;
    }

    public StatisticsContainer addInPlace(StatisticsContainer arg0, StatisticsContainer arg1) {
        return addAccumulator(arg0, arg1);
    }

    public StatisticsContainer zero(StatisticsContainer arg0) {
        return new StatisticsContainer();
    }
}
