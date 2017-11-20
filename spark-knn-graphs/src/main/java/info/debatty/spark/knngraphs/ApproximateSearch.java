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
   public final NeighborList search(
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
    public final NeighborList search(
            final T query,
            final FastSearchConfig conf) {

        Node<T> query_node = new Node<>();
        query_node.value = query;

        JavaRDD<FastSearchResult> candidates_neighborlists
                = distributed_graph.map(
                        new DistributedSearch(query_node, conf));

        NeighborList final_neighborlist = new NeighborList(conf.getK());
        for (FastSearchResult<T> result : candidates_neighborlists.collect()) {
            final_neighborlist.addAll(result.getNeighbors());
        }
        return final_neighborlist;
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

    DistributedSearch(
            final Node<T> query,
            final FastSearchConfig conf) {

        this.query = query;
        this.conf = conf;
    }

    @Override
    public FastSearchResult<T> call(final Graph<Node<T>> local_graph) {

        FastSearchResult local_results = local_graph.fastSearch(
                query,
                conf);

        return local_results;
    }
}
