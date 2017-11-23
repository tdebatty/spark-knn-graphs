/*
 * The MIT License
 *
 * Copyright 2015 Thibault Debatty.
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

import info.debatty.java.graphs.FastSearchConfig;
import info.debatty.java.graphs.FastSearchResult;
import info.debatty.java.graphs.Graph;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.spark.knngraphs.partitioner.KMedoids;
import java.util.LinkedList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

/**
 * Perform a fast distributed nn search, first partitioning the graph.
 * @author Thibault Debatty
 * @param <T>
 */
public class ApproximateSearch<T> {

    // State: the partitioned graph
    private final JavaRDD<Graph<Node<T>>> distributed_graph;

    /**
     * Prepare the graph for distributed search.
     *
     * @param graph
     * @param similarity
     * @param partitions
     */
    public ApproximateSearch(
            final JavaPairRDD<Node<T>, NeighborList> graph,
            final SimilarityInterface<T> similarity,
            final int partitions) {

        // Partition the graph
        KMedoids<T> partitioner
                = new KMedoids<>(similarity, partitions);
        JavaPairRDD<Node<T>, NeighborList> partitioned_graph =
                partitioner.partition(graph).wrapped_graph;

        this.distributed_graph = DistributedGraph.toGraph(
                partitioned_graph, similarity);
        this.distributed_graph.cache();
        this.distributed_graph.count();
    }

    /**
     * Use the graph as it is, without repartitioning.
     * @param distributed_graph
     */
    public ApproximateSearch(final JavaRDD<Graph<Node<T>>> distributed_graph) {
        this.distributed_graph = distributed_graph;
    }

    /**
     * Unpersist the internal cached RDD.
     */
    public final void clean() {
        distributed_graph.unpersist(true);
    }

   /**
    * Perform fast distributed search using default parameters.
    *
    * @param query
    * @param k
    * @return
    */
    public final FastSearchResult<Node<T>> search(
            final T query,
            final int k) {
       return search(
               query,
               FastSearchConfig.getDefault());
   }

    /**
     *
     * @param query
     * @param conf
     * @return
     */
    public final FastSearchResult<Node<T>> search(
            final T query,
            final FastSearchConfig conf) {

        if (!conf.isRestartAtBoundary()) {
            // This is a naive search: the sequential search algorithm will
            // return if the number of computed similarities reaches the max
            // OR if the search reaches the boundary of the partition

            return naiveSearch(query, conf);
        }

        Node<T> query_node = new Node<>(query);

        JavaRDD<FastSearchResult<Node<T>>> results = distributed_graph.map(
                        new DistributedSearch(
                                query_node, conf, new LinkedList<>()));

        FastSearchResult<Node<T>> global_result =
                new FastSearchResult<>(conf.getK());
        global_result.addAll(results.collect());
        return global_result;
    }

    /**
     *
     * @param query
     * @param conf
     * @return
     */
    public final FastSearchResult<Node<T>> naiveSearch(
            final T query, final FastSearchConfig conf) {

        long max_similarities = DistributedGraph.size(distributed_graph)
                / (long) conf.getSpeedup();

        LinkedList<Node<T>> starting_points = new LinkedList<>();
        Node<T> query_node = new Node<>(query);
        FastSearchResult<Node<T>> global_result =
                new FastSearchResult<>(conf.getK());

        while (global_result.getSimilarities() < max_similarities) {
            JavaRDD<FastSearchResult> results = distributed_graph.map(
                        new DistributedSearch(
                                query_node, conf, starting_points));

            starting_points = new LinkedList<>();

            for (FastSearchResult<Node<T>> result : results.collect()) {
                global_result.add(result);

                if (result.getBoundaryNode() != null) {
                    starting_points.add(result.getBoundaryNode());
                }
            }
        }

        return global_result;
    }
}

/**
 * Used for searching the distributed graph (RDD<Graph>).
 * @author Thibault Debatty
 * @param <T> Value of graph nodes
 */
class DistributedSearch<T>
        implements Function<Graph<Node<T>>, FastSearchResult<T>> {

    private final Node<T> query;
    private final FastSearchConfig conf;
    private final LinkedList<Node<T>> starting_points;

    DistributedSearch(
            final Node<T> query,
            final FastSearchConfig conf,
            final LinkedList<Node<T>> starting_points) {

        this.query = query;
        this.conf = conf;
        this.starting_points = starting_points;
    }

    @Override
    public FastSearchResult<T> call(final Graph<Node<T>> local_graph) {

        for (Node<T> start : starting_points) {
            if (local_graph.containsKey(start)) {
                return local_graph.fastSearch(query, conf, start);
            }
        }

        return local_graph.fastSearch(query, conf);
    }
}
