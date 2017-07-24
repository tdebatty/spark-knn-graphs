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
import org.apache.spark.util.AccumulatorV2;

/**
 *
 * @author Thibault Debatty
 */
public class StatisticsAccumulator
        extends AccumulatorV2<StatisticsContainer, StatisticsContainer> {

    private StatisticsContainer internal = new StatisticsContainer();

    /**
     *
     */
    public StatisticsAccumulator() {
    }

    @Override
    public final boolean isZero() {
        return internal.getSimilarities() == 0;
    }

    @Override
    public final AccumulatorV2<StatisticsContainer, StatisticsContainer> copy() {
        StatisticsAccumulator acc = new StatisticsAccumulator();
        acc.add(internal);
        return acc;
    }

    @Override
    public final void reset() {
        this.internal = new StatisticsContainer();
    }

    @Override
    public final void add(final StatisticsContainer stats) {
        this.internal.incAddSimilarities(stats.getAddSimilarities());
        this.internal.incRemoveSimilarities(stats.getRemoveSimilarities());
        this.internal.incSearchCrossPartitionRestarts(
                stats.getSearchCrossPartitionRestarts());
        this.internal.incSearchRestarts(stats.getSearchRestarts());
        this.internal.incSearchSimilarities(stats.getSearchSimilarities());
    }

    @Override
    public final void merge(
            final AccumulatorV2<StatisticsContainer, StatisticsContainer>
                    other) {
        this.add(other.value());
    }

    @Override
    public final StatisticsContainer value() {
        return internal;
    }
}
