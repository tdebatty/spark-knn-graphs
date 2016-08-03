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
package info.debatty.spark.knngraphs;

import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import java.util.Iterator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

/**
 *
 * @author Thibault Debatty
 */
public class Graph {

    /**
     *
     * @param <T>
     * @param graph1
     * @param graph2
     * @return
     */
    public static final <T> long countCommonEdges(
            final JavaPairRDD<Node<T>, NeighborList> graph1,
            final JavaPairRDD<Node<T>, NeighborList> graph2) {

        return (Long) graph1
                .union(graph2)
                .groupByKey()
                .map(new CountFunction())
                .reduce(new SumFunction());
    }
}

/**
 *
 * @author Thibault Debatty
 * @param <T>
 */
class CountFunction<T>
        implements Function<Tuple2<Node<T>, Iterable<NeighborList>>, Long> {

    /**
     *
     * @param tuples
     * @return
     */
    public Long call(final Tuple2<Node<T>, Iterable<NeighborList>> tuples) {
        Iterator<NeighborList> iterator = tuples._2.iterator();
        NeighborList nl1 = iterator.next();
        NeighborList nl2 = iterator.next();
        return new Long(nl1.countCommons(nl2));
    }

}

/**
 *
 * @author Thibault Debatty
 */
class SumFunction
        implements Function2<Long, Long, Long> {

    /**
     *
     * @param arg0
     * @param arg1
     * @return
     * @throws Exception
     */
    public Long call(final Long arg0, final Long arg1) {
        return arg0 + arg1;
    }

}