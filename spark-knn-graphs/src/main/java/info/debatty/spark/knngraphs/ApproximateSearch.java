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
import java.util.List;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

/**
 *
 * @author Thibault Debatty
 * @param <T>
 */
public class ApproximateSearch<T> {

    private static final double DEFAULT_IMBALANCE = 1.1;

    /**
     * Default speedup compared to exhaustive search.
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

    private JavaRDD<Graph<T>> distributed_graph;
    private final BalancedKMedoidsPartitioner partitioner;

    /**
     * Prepare the graph for distributed search using default imbalance.
     * Default imbalance = 1.1
     * @param graph
     * @param partitioning_iterations
     * @param partitioning_medoids
     * @param similarity
     */
    public ApproximateSearch(
            final JavaPairRDD<Node<T>, NeighborList> graph,
            final int partitioning_iterations,
            final int partitioning_medoids,
            final SimilarityInterface<T> similarity) {

        this(
                graph,
                partitioning_iterations,
                partitioning_medoids,
                similarity,
                DEFAULT_IMBALANCE);
    }

    /**
     * Prepare the graph for distributed search.
     *
     * @param graph
     * @param partitioning_iterations
     * @param partitioning_medoids
     * @param similarity
     * @param imbalance
     */
    public ApproximateSearch(
            final JavaPairRDD<Node<T>, NeighborList> graph,
            final int partitioning_iterations,
            final int partitioning_medoids,
            final SimilarityInterface<T> similarity,
            final double imbalance) {

        // Partition the graph
        this.partitioner = new BalancedKMedoidsPartitioner();
        partitioner.setIterations(partitioning_iterations);
        partitioner.setPartitions(partitioning_medoids);
        partitioner.setSimilarity(similarity);
        partitioner.setImbalance(imbalance);

        this.distributed_graph = partitioner.partition(graph);
        this.distributed_graph.cache();
        this.distributed_graph.count();
    }

    /**
     * Unpersist the internal cached RDD.
     */
    public final void clean() {
        distributed_graph.unpersist(true);
    }

    /**
     *
     * @return
     */
    public final BalancedKMedoidsPartitioner getPartitioner() {
        return partitioner;
    }

    /**
     *
     * @return
     */
    public final List<Node<T>> getMedoids() {
        return partitioner.getMedoids();
    }

    /**
     * Assign the node to correct partition.
     * @param node
     * @param counts
     */
    public final void assign(final Node<T> node, final long[] counts) {
        partitioner.assign(node, counts);
    }

    /**
     * Fast distributed search with default number of random jumps (2) and
     * expansion (1.2).
     * @param query
     * @param k
     * @param speedup
     * @return
     */
   public final NeighborList search(
            final Node<T> query,
            final int k,
            final double speedup) {
       return search(query, k, speedup, DEFAULT_JUMPS, DEFAULT_EXPANSION);
   }

   /**
    * Fast distributed search with default speedup compared to exhaustive search
    * (10), number of random jumps (2) and expansion (1.2).
    * @param query
    * @param k
    * @return
    */
   public final NeighborList search(
            final Node<T> query,
            final int k) {
       return search(query, k, DEFAULT_SPEEDUP);
   }

   /**
     *
     * @param query
     * @param k
     * @param speedup
     * @param random_jumps
     * @param expansion
     * @return
     */
    public final NeighborList search(
            final Node<T> query,
            final int k,
            final double speedup,
            final int random_jumps,
            final double expansion) {

        return search(query, k, speedup, random_jumps, expansion, null);
    }

    /**
     *
     * @param query
     * @param k
     * @param speedup
     * @param random_jumps
     * @param expansion
     * @param stats_accumulator
     * @return
     */
    public final NeighborList search(
            final Node<T> query,
            final int k,
            final double speedup,
            final int random_jumps,
            final double expansion,
            final Accumulator<StatisticsContainer> stats_accumulator) {

        JavaRDD<NeighborList> candidates_neighborlists
                = distributed_graph.map(
                        new DistributedSearch(
                                query,
                                k,
                                speedup,
                                random_jumps,
                                expansion,
                                stats_accumulator));

        NeighborList final_neighborlist = new NeighborList(k);
        for (NeighborList nl : candidates_neighborlists.collect()) {
            final_neighborlist.addAll(nl);
        }
        return final_neighborlist;
    }

    /**
     *
     * @return
     */
    public final JavaRDD<Graph<T>> getGraph() {
        return this.distributed_graph;
    }

    /**
     *
     * @param graph
     */
    public final void setGraph(final JavaRDD<Graph<T>> graph) {
        this.distributed_graph = graph;
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
    private final Accumulator<StatisticsContainer> stats_accumulator;

    DistributedSearch(
            final Node<T> query,
            final int k,
            final double speedup,
            final int random_jumps,
            final double expansion,
            final Accumulator<StatisticsContainer> stats_accumulator) {

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
