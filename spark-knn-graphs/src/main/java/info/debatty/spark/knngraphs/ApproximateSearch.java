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

import info.debatty.java.graphs.Graph;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.java.graphs.StatisticsContainer;
import info.debatty.spark.knngraphs.builder.StatisticsAccumulator;
import info.debatty.spark.knngraphs.partitioner.KMedoids;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

/**
 * Perform a fast distributed nn search, but does not care for partitioning the
 * graph.
 * @author Thibault Debatty
 * @param <T>
 */
public class ApproximateSearch<T> {

    /**
     * Default search speedup compared to exhaustive search.
     */
    public static final int DEFAULT_SPEEDUP = 10;

    /**
     * Default number of random jumps per node when searching.
     */
    public static final int DEFAULT_JUMPS = 2;

    /**
     * Default value for search expansion parameter.
     */
    public static final double DEFAULT_EXPANSION = 1.2;

    // Fast search parameters
    private final JavaRDD<Graph<T>> distributed_graph;

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
                = new KMedoids<T>(similarity, partitions);
        JavaPairRDD<Node<T>, NeighborList> partitioned_graph =
                partitioner.partition(graph).graph;

        this.distributed_graph = DistributedGraph.toGraph(
                partitioned_graph, similarity);
        this.distributed_graph.cache();
        this.distributed_graph.count();
    }

    /**
     * Use the graph as it is, without repartitioning.
     * @param distributed_graph
     */
    public ApproximateSearch(final JavaRDD<Graph<T>> distributed_graph) {
        this.distributed_graph = distributed_graph;
    }

    /**
     * Unpersist the internal cached RDD.
     */
    public final void clean() {
        distributed_graph.unpersist(true);
    }

   /**
    * Fast distributed search.
    * @param query
    * @param k
    * @return
    */
   public final NeighborList search(
            final Node<T> query,
            final int k) {
       return search(
               query,
               k,
               null,
               DEFAULT_SPEEDUP,
               DEFAULT_JUMPS,
               DEFAULT_EXPANSION);
   }

    /**
     *
     * @param query
     * @param k
     * @param stats_accumulator
     * @param speedup
     * @param jumps
     * @param expansion
     * @return
     */
    public final NeighborList search(
            final Node<T> query,
            final int k,
            final StatisticsAccumulator stats_accumulator,
            final double speedup,
            final int jumps,
            final double expansion) {

        JavaRDD<NeighborList> candidates_neighborlists
                = distributed_graph.map(
                        new DistributedSearch(
                                query,
                                k,
                                speedup,
                                jumps,
                                expansion,
                                stats_accumulator));

        NeighborList final_neighborlist = new NeighborList(k);
        for (NeighborList nl : candidates_neighborlists.collect()) {
            final_neighborlist.addAll(nl);
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
        implements Function<Graph<T>, NeighborList> {

    private final double speedup;
    private final int k;
    private final Node<T> query;
    private final int random_jumps;
    private final double expansion;
    private final StatisticsAccumulator stats_accumulator;

    DistributedSearch(
            final Node<T> query,
            final int k,
            final double speedup,
            final int random_jumps,
            final double expansion,
            final StatisticsAccumulator stats_accumulator) {

        this.query = query;
        this.k = k;
        this.speedup = speedup;
        this.random_jumps = random_jumps;
        this.expansion = expansion;
        this.stats_accumulator = stats_accumulator;
    }

    public NeighborList call(final Graph<T> local_graph) throws Exception {

        StatisticsContainer local_stats = new StatisticsContainer();

        NeighborList local_results = local_graph.fastSearch(
                query.value,
                k,
                speedup,
                random_jumps,
                expansion,
                local_stats);

        if (stats_accumulator != null) {
            stats_accumulator.add(local_stats);
        }

        return local_results;
    }
}
