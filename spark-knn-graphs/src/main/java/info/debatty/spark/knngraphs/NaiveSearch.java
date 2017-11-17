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
package info.debatty.spark.knngraphs;

import info.debatty.java.graphs.Graph;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.SimilarityInterface;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

/**
 * Naive distributed search.
 * At each iteration the local subgraph (partition) is searched. When the search
 * reaches the boundary of a partition, the boundary point is used as starting
 * point at the next iteration...
 *
 * @author Thibault Debatty
 * @param <T> type of data in the graph
 */
public class NaiveSearch<T> {

    private final JavaRDD<Graph<Node<T>>> distributed_graph;

    /**
     *
     * @param graph
     * @param similarity
     */
    public NaiveSearch(
            final JavaPairRDD<Node<T>, NeighborList> graph,
            final SimilarityInterface<T> similarity) {

        // Transform into an RDD of graphs
        this.distributed_graph = DistributedGraph.toGraph(graph, similarity);
        this.distributed_graph.cache();
        this.distributed_graph.count();

    }

    public final NeighborList search(
            final T query,
            final int k) {

        this.distributed_graph.map(new NaiveDistributedSearch());

        return null;
    }


}

class NaiveDistributedSearch<T>
        implements Function<Graph<T>, NeighborList> {

    @Override
    public NeighborList call(final Graph<T> graph) {
        // graph.fa
        return null;
    }

}