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

import info.debatty.java.graphs.Graph;
import info.debatty.java.graphs.Neighbor;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.spark.knngraphs.ApproximateSearch;
import info.debatty.spark.knngraphs.BalancedKMedoidsPartitioner;
import info.debatty.spark.knngraphs.NodePartitioner;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

/**
 *
 * @author Thibault Debatty
 * @param <T>
 */
public class Online<T> {

    private static final int PARTITIONING_ITERATIONS = 5;
    private static final int PARTITIONING_MEDOIDS = 4;
    private static final int SEARCH_SPEEDUP = 4;
    private static final int ITERATIONS_FOR_CHECKPOINT = 20;
    private static final int ITERATIONS_FOR_CENTROIDS = 100;

    private final ApproximateSearch<T> searcher;
    private final int k;
    private final JavaSparkContext sc;
    private final SimilarityInterface<T> similarity;
    private final long[] counts;
    private int iteration;

    /**
     *
     * @param k number of edges per node
     * @param similarity similarity to use for computing edges
     * @param sc spark context
     * @param initial initial graph
     */
    public Online(
            final int k,
            final SimilarityInterface<T> similarity,
            final JavaSparkContext sc,
            final JavaPairRDD<Node<T>, NeighborList> initial) {

        this.iteration = 0;
        this.similarity = similarity;
        this.k = k;
        this.sc = sc;
        this.searcher = new ApproximateSearch<T>(
                initial,
                PARTITIONING_ITERATIONS,
                PARTITIONING_MEDOIDS,
                similarity);

        this.counts = getCounts();
    }

    /**
     *
     * @param node to add to the graph
     */
    public final void addNode(final Node<T> node) {
        iteration++;

        // Find the neighbors of this node
        NeighborList neighborlist = searcher.search(node, k, SEARCH_SPEEDUP);

        // Parallelize the pair node => neighborlist
        LinkedList<Tuple2<Node<T>, NeighborList>> list
                = new LinkedList<Tuple2<Node<T>, NeighborList>>();
        list.add(new Tuple2<Node<T>, NeighborList>(node, neighborlist));
        JavaPairRDD<Node<T>, NeighborList> new_graph_piece
                = sc.parallelizePairs(list);

        // Partition the pair
        JavaPairRDD<Node<T>, NeighborList> partitioned_piece = partition(
                new_graph_piece,
                searcher.getMedoids(),
                counts,
                searcher.getPartitioner().getInternalPartitioner());
        partitioned_piece.cache();

        // bookkeeping: update the counts
        Node<T> partitioned_node = partitioned_piece.collect().get(0)._1;
        counts[(Integer)
                partitioned_node
                .getAttribute(BalancedKMedoidsPartitioner.PARTITION_KEY)]++;

        // update the existing graph
        JavaPairRDD<Node<T>, NeighborList> updated_graph =
                update(searcher.getGraph(), node, neighborlist);

        // The new graph is the union of Java RDD's
        JavaPairRDD<Node<T>, NeighborList> union =
                updated_graph.union(partitioned_piece);
        union.cache();

        //  truncate RDD DAG (would cause a stack overflow, even with caching)
        if ((iteration % ITERATIONS_FOR_CHECKPOINT) == 0) {
            union.rdd().localCheckpoint();
        }

        if ((iteration % ITERATIONS_FOR_CENTROIDS) == 0) {
            searcher.getPartitioner().computeNewMedoids(union);
        }

        // From now on, use the new graph...
        searcher.setGraph(union);
    }

    /**
     *
     * @return the current graph
     */
    public final JavaPairRDD<Node<T>, NeighborList> getGraph() {
        return searcher.getGraph();
    }

    private JavaPairRDD<Node<T>, NeighborList> update(
            final JavaPairRDD<Node<T>, NeighborList> graph,
            final Node<T> node,
            final NeighborList neighborlist) {

        return graph.mapPartitionsToPair(
                new UpdateFunction<T>(node, neighborlist, similarity),
                true);
    }

    /**
     *
     * @param <U>
     */
    private static class UpdateFunction<U>
            implements PairFlatMapFunction
            <Iterator<Tuple2<Node<U>, NeighborList>>, Node<U>, NeighborList> {

        private static final int EXPANSION_LEVELS = 3;
        private final NeighborList neighborlist;
        private final SimilarityInterface<U> similarity;
        private final Node<U> node;

        public UpdateFunction(
                final Node<U> node,
                final NeighborList neighborlist,
                final SimilarityInterface<U> similarity) {

            this.node = node;
            this.neighborlist = neighborlist;
            this.similarity = similarity;
        }

        public Iterable<Tuple2<Node<U>, NeighborList>> call(
                final Iterator<Tuple2<Node<U>, NeighborList>> iterator)
                throws Exception {

            // Rebuild the local graph
            Graph<U> local_graph = new Graph<U>();
            while (iterator.hasNext()) {
                Tuple2<Node<U>, NeighborList> tuple = iterator.next();
                local_graph.put(
                        tuple._1, tuple._2);
            }

            // Nodes to analyze at this iteration
            LinkedList<Node<U>> analyze = new LinkedList<Node<U>>();

            // Nodes to analyze at next iteration
            LinkedList<Node<U>> next_analyze = new LinkedList<Node<U>>();

            // List of already analyzed nodes
            HashMap<Node<U>, Boolean> visited = new HashMap<Node<U>, Boolean>();

            // Fill the list of nodes to analyze
            for (Neighbor neighbor : neighborlist) {
                analyze.add(neighbor.node);
            }

            for (int level = 0; level < EXPANSION_LEVELS; level++) {
                while (!analyze.isEmpty()) {
                    Node other = analyze.pop();
                    NeighborList other_neighborlist = local_graph.get(other);

                    // This part of the graph is in another partition :-(
                    if (other_neighborlist == null) {
                        continue;
                    }

                    // Add neighbors to the list of nodes to analyze
                    // at next iteration
                    for (Neighbor other_neighbor : other_neighborlist) {
                        if (!visited.containsKey(other_neighbor.node)) {
                            next_analyze.add(other_neighbor.node);
                        }
                    }

                    // Try to add the new node (if sufficiently similar)
                    other_neighborlist.add(new Neighbor(
                            node,
                            similarity.similarity(
                                    node.value,
                                    (U) other.value)));

                    visited.put(other, Boolean.TRUE);
                }

                analyze = next_analyze;
                next_analyze = new LinkedList<Node<U>>();
            }

            // Return the resulting nodes => neighborlist
            ArrayList<Tuple2<Node<U>, NeighborList>> r =
                    new ArrayList<Tuple2<Node<U>, NeighborList>>(
                            local_graph.size());

            for (Node<U> n : local_graph.getNodes()) {
                r.add(new Tuple2<Node<U>, NeighborList>(n, local_graph.get(n)));
            }

            return r;
        }

    }

    /**
     * Perform a single iteration of partitioning, without recomputing new
     * medoids.
     *
     * @param piece RDD containing subgraph to partition
     * @param medoids medoids to use for partitioning
     * @param counts current number of nodes per partition (used for bounding)
     * @param internal_partitioner internal spark partitioner object
     * @return a copy of the RDD, partitioned
     */
    private JavaPairRDD<Node<T>, NeighborList> partition(
            final JavaPairRDD<Node<T>, NeighborList> piece,
            final List<Node<T>> medoids,
            final long[] counts,
            final NodePartitioner internal_partitioner) {

        // Assign each node to a partition id
        JavaPairRDD<Node<T>, NeighborList> partitioned_graph
                = piece.mapPartitionsToPair(
                        new AssignFunction<T>(medoids, counts, similarity),
                        true);

        // Partition
        partitioned_graph = partitioned_graph.partitionBy(internal_partitioner);

        return partitioned_graph;
    }

    /**
     *
     * @param <U>
     */
    private static class AssignFunction<U>
            implements PairFlatMapFunction
            <Iterator<Tuple2<Node<U>, NeighborList>>, Node<U>, NeighborList> {

        private static final double IMBALANCE = 1.1;

        private final List<Node<U>> medoids;
        private final long[] counts;
        private final SimilarityInterface<U> similarity;

        public AssignFunction(
                final List<Node<U>> medoids,
                final long[] counts,
                final SimilarityInterface<U> similarity) {
            this.medoids = medoids;
            this.counts = counts;
            this.similarity = similarity;
        }

        public Iterable<Tuple2<Node<U>, NeighborList>>
                call(final Iterator<Tuple2<Node<U>, NeighborList>> iterator)
                throws Exception {

            // Total number of elements
            long n = sum(counts) + 1;
            int partitions = medoids.size();
            int partition_constraint = (int) (IMBALANCE * n / partitions);

            // fetch all tuples in this partition
            // to compute the partition_constraint
            ArrayList<Tuple2<Node<U>, NeighborList>> tuples
                    = new ArrayList<Tuple2<Node<U>, NeighborList>>();

            while (iterator.hasNext()) {
                Tuple2<Node<U>, NeighborList> tuple = iterator.next();
                tuples.add(tuple);

                double[] similarities = new double[partitions];
                double[] values = new double[partitions];

                // 1. similarities
                for (int center_id = 0; center_id < partitions; center_id++) {
                    similarities[center_id] = similarity.similarity(
                            medoids.get(center_id).value,
                            tuple._1.value);
                }

                // 2. value to maximize :
                // similarity * (1 - cluster_size / capacity_constraint)
                for (int center_id = 0; center_id < partitions; center_id++) {
                    values[center_id] = similarities[center_id]
                            * (1 - counts[center_id] / partition_constraint);
                }

                // 3. choose partition that minimizes compute value
                int partition = argmax(values);
                counts[partition]++;
                tuple._1.setAttribute(
                        BalancedKMedoidsPartitioner.PARTITION_KEY,
                        partition);
            }

            return tuples;
        }

        private static long sum(final long[] values) {
            long agg = 0;
            for (long value : values) {
                agg += value;
            }
            return agg;
        }

        private static int argmax(final double[] values) {
            double max_value = -1.0 * Double.MAX_VALUE;
            ArrayList<Integer> ties = new ArrayList<Integer>();

            for (int i = 0; i < values.length; i++) {
                if (values[i] > max_value) {
                    max_value = values[i];
                    ties = new ArrayList<Integer>();
                    ties.add(i);

                } else if (values[i] == max_value) {
                    // add a tie
                    ties.add(i);
                }
            }

            if (ties.size() == 1) {
                return ties.get(0);
            }

            Random rand = new Random();
            return ties.get(rand.nextInt(ties.size()));
        }
    }

    private long[] getCounts() {
        List<Long> counts_list = searcher.getGraph().mapPartitions(
                new PartitionCountFunction(), true).collect();

        long[] result = new long[counts_list.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = counts_list.get(i);
        }
        return result;
    }

    /**
     *
     * @param <U>
     */
    private static class PartitionCountFunction<U>
            implements FlatMapFunction
            <Iterator<Tuple2<Node<U>, NeighborList>>, Long> {

        /**
         *
         * @param iterator
         * @return
         * @throws Exception
         */
        public Iterable<Long> call(
                final Iterator<Tuple2<Node<U>, NeighborList>> iterator)
                throws Exception {
            long count = 0;
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }

            ArrayList<Long> result = new ArrayList<Long>(1);
            result.add(count);
            return result;
        }
    }
}
