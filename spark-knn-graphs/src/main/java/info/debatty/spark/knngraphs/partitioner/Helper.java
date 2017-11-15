/*
 * The MIT License
 *
 * Copyright 2017 tibo.
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
package info.debatty.spark.knngraphs.partitioner;

import info.debatty.spark.knngraphs.Node;
import info.debatty.java.graphs.NeighborList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 *
 * @author tibo
 */
public class Helper {



    public static final <T> JavaPairRDD<T, NeighborList> unwrapNodes(
            final JavaPairRDD<Node<T>, NeighborList> graph) {

        return graph.mapToPair(new UnwrapNodeFunction());
    }

    /**
     * Move the nodes to the correct partition.
     * Does NOT force execution or cache the resulting partition!
     * @param graph
     * @param partitions
     * @return
     */
    public static final <T> JavaPairRDD<Node<T>, NeighborList> moveNodes(
            final JavaPairRDD<Node<T>, NeighborList> graph,
            final int partitions) {

        return graph.partitionBy(new NodePartitioner(partitions));
    }
}




class UnwrapNodeFunction<T> implements PairFunction<
        Tuple2<Node<T>, NeighborList>, T, NeighborList> {

    @Override
    public Tuple2<T, NeighborList> call(
            final Tuple2<Node<T>, NeighborList> arg) {

        return new Tuple2<>(
                arg._1.value,
                arg._2);
    }
}
