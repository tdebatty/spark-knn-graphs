/*
 * The MIT License
 *
 * Copyright 2017 Thibault Debatty.
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
 * @author Thibault Debatty
 * @param <T>
 */
public class Edge1D<T> implements Partitioner<T> {

    private final int partitions;

    /**
     *
     * @param partitions
     */
    public Edge1D(final int partitions) {
        this.partitions = partitions;
    }

    /**
     * {@inheritDoc}
     * @param graph
     * @return
     */
    @Override
    public final Partitioning<T> partition(
            final JavaPairRDD<Node<T>, NeighborList> graph) {

        Partitioning<T> solution = new Partitioning<>();
        solution.start_time = System.currentTimeMillis();


        JavaPairRDD<Node<T>, NeighborList> partitioned_graph =
                graph.mapToPair(new Edge1DFunction<T>(partitions));

        JavaPairRDD<Node<T>, NeighborList> distributed_graph =
                Helper.moveNodes(partitioned_graph, partitions);

        solution.wrapped_graph = distributed_graph;
        solution.wrapped_graph.cache();
        solution.wrapped_graph.count();
        solution.end_time = System.currentTimeMillis();
        return solution;
    }
}

/**
 * Assign a partition id to each node, using the node id.
 * @author tibo
 * @param <T>
 */
class Edge1DFunction<T>
        implements PairFunction<
            Tuple2<Node<T>, NeighborList>,
            Node<T>,
            NeighborList> {

    private static final long MIXING_PRIME = 1125899906842597L;
    private final int partitions;

    Edge1DFunction(final int partitions) {
        this.partitions = partitions;
    }


    @Override
    public Tuple2<Node<T>, NeighborList> call(
            final Tuple2<Node<T>, NeighborList> tuple) {

        Node<T> node = tuple._1;
        int partition = (int) (Math.abs(node.id * MIXING_PRIME) % partitions);
        node.partition = partition;
        return tuple;
    }
}