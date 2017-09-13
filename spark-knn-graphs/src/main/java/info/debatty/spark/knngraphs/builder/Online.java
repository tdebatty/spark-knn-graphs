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
import info.debatty.java.graphs.StatisticsContainer;
import info.debatty.spark.knngraphs.ApproximateSearch;
import info.debatty.spark.knngraphs.DistributedGraph;
import info.debatty.spark.knngraphs.partitioner.KMedoids;
import info.debatty.spark.knngraphs.partitioner.KMedoidsPartitioning;
import info.debatty.spark.knngraphs.partitioner.NodePartitioner;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

/**
 *
 * @author Thibault Debatty
 * @param <T>
 */
public class Online<T> {

    // ------ Default values ------
    private static final int DEFAULT_UPDATE_DEPTH = 2;
    private static final double DEFAULT_MEDOID_UPDATE_RATIO = 0.1;

    // Number of nodes to add before performing a checkpoint
    // (to strip RDD DAG)
    private static final int ITERATIONS_BEFORE_CHECKPOINT = 100;

    // Number of RDD's to cache
    private static final int RDDS_TO_CACHE = 3;

    // ------ Algorithm parameters ------
    private final JavaSparkContext spark_context;
    private final int k;
    private final SimilarityInterface<T> similarity;

    // Number of nodes to add before recomputing centroids
    private final double medoid_update_ratio;

    // Parameters used to add or remove nodes
    private final double search_speedup;
    private final int search_random_jumps;
    private final double search_expansion;
    private final int update_depth;
    private final double imbalance = 1.1;

    // ------ Algorithm state ------
    private final long[] partitions_size;
    private final LinkedList<JavaRDD<Graph<T>>> previous_rdds;
    private long nodes_added_or_removed = 0;
    private long nodes_before_update_medoids = 0;
    private JavaRDD<Graph<T>> distributed_graph;
    private final ArrayList<Node<T>> medoids;

    /**
     *
     * @param k
     * @param similarity
     * @param sc
     * @param initial_graph
     * @param partitioning_medoids
     */
    public Online(
            final int k,
            final SimilarityInterface<T> similarity,
            final JavaSparkContext sc,
            final JavaPairRDD<Node<T>, NeighborList> initial_graph,
            final int partitioning_medoids) {

        this.similarity = similarity;
        this.k = k;
        this.spark_context = sc;

        this.medoid_update_ratio = DEFAULT_MEDOID_UPDATE_RATIO;
        this.search_speedup = ApproximateSearch.DEFAULT_SPEEDUP;
        this.search_random_jumps = ApproximateSearch.DEFAULT_JUMPS;
        this.search_expansion = ApproximateSearch.DEFAULT_EXPANSION;
        this.update_depth = DEFAULT_UPDATE_DEPTH;

        this.previous_rdds = new LinkedList<JavaRDD<Graph<T>>>();

        // Use kmedoids to partition the graph
        KMedoids<T> partitioner
                = new KMedoids<T>(similarity, partitioning_medoids);

        KMedoidsPartitioning<T> partitioning =
                partitioner.partition(initial_graph);

        this.medoids = partitioning.medoids;
        this.distributed_graph = DistributedGraph.toGraph(
                partitioning.graph, similarity);
        this.distributed_graph.cache();
        this.distributed_graph.count();

        this.partitions_size = getPartitionsSize(distributed_graph);
        this.nodes_before_update_medoids = computeNodesBeforeUpdate();

    }

    /**
     * Unpersist all cached RDD's.
     */
    public final void clean() {
        for (JavaRDD<Graph<T>> rdd : previous_rdds) {
            rdd.unpersist();
        }
    }

    /**
     * Get the total number of nodes in the online graph.
     * @return total number of nodes in the graph
     */
    public final long getSize() {
        long agg = 0;
        for (long value : partitions_size) {
            agg += value;
        }
        return agg;
    }

    /**
     * Add a node to the graph using fast distributed algorithm.
     * @param node to add to the graph
     */
    public final void fastAdd(final Node<T> node) {
        fastAdd(node, null);
    }

    /**
     * Add a node to the graph using fast distributed algorithm.
     * @param node to add to the graph
     * @param stats_accumulator
     */
    public final void fastAdd(
            final Node<T> node,
            final StatisticsAccumulator stats_accumulator) {

        // Find the neighbors of this node
        ApproximateSearch<T> searcher = new ApproximateSearch<T>(
                distributed_graph);
        NeighborList neighborlist = searcher.search(
                node,
                k,
                null,
                search_speedup,
                search_random_jumps,
                search_expansion);

        // Assign the node to a partition (most similar medoid, with partition
        // size constraint)
        assign(node);

        // bookkeeping: update the counts
        partitions_size[(Integer) node
                .getAttribute(NodePartitioner.PARTITION_KEY)]++;

        // update the existing graph edges
        JavaRDD<Graph<T>> updated_graph = distributed_graph.map(
                    new UpdateFunction<T>(
                            node,
                            neighborlist,
                            similarity,
                            stats_accumulator,
                            update_depth));

        // Add the new <Node, NeighborList> to the distributed graph
        updated_graph = updated_graph.map(new AddNode(node, neighborlist));
        updated_graph = updated_graph.cache();

        //  truncate RDD DAG (would cause a stack overflow, even with caching)
        if ((nodes_added_or_removed % ITERATIONS_BEFORE_CHECKPOINT) == 0) {
            updated_graph.rdd().localCheckpoint();
        }

        // Keep a track of updated RDD to unpersist after two iterations
        previous_rdds.add(updated_graph);
        if (nodes_added_or_removed > RDDS_TO_CACHE) {
            previous_rdds.pop().unpersist();
        }

        // Force execution to get stats accumulator values
        updated_graph.count();

        // From now on use the new graph...
        this.distributed_graph = updated_graph;

        nodes_added_or_removed++;
        nodes_before_update_medoids--;
        if (nodes_before_update_medoids == 0) {
            // TODO: count number of computed similarities here!!
            // computeNewMedoids(updated_graph);
            nodes_before_update_medoids = computeNodesBeforeUpdate();
        }
    }

    private final void assign(final Node<T> node) {
        // Total number of elements
        long n = sum(partitions_size) + 1;
        int partitions = medoids.size();
        int partition_constraint = (int) (imbalance * n / partitions);

        double[] similarities = new double[partitions];
        double[] values = new double[partitions];

        // 1. similarities
        for (int i = 0; i < partitions; i++) {
            similarities[i] = similarity.similarity(
                    medoids.get(i).value,
                    node.value);
        }

        // 2. value to maximize :
        // similarity * (1 - cluster_size / capacity_constraint)
        for (int center_id = 0; center_id < partitions; center_id++) {
            values[center_id] = similarities[center_id]
                    * (1 - partitions_size[center_id] / partition_constraint);
        }

        // 3. choose partition that maximizes computed value
        int partition = argmax(values);
        partitions_size[partition]++;
        node.setAttribute(
                NodePartitioner.PARTITION_KEY,
                partition);

    }

    /*
    private final void computeNewMedoids(
            final JavaRDD<Graph<T>> distributed_graph) {
        medoids = distributed_graph.map(
                new ComputeMedoids()).collect();
    }*/

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

    /**
     * Remove a node using fast approximate algorithm.
     * @param node_to_remove
     * @param stats_accumulator
     */
    public final void fastRemove(
            final Node<T> node_to_remove,
            final StatisticsAccumulator stats_accumulator) {

        // find the list of nodes to update
        List<Node<T>> nodes_to_update = distributed_graph
                .flatMap(new FindNodesToUpdate(node_to_remove))
                .collect();

        // build the list of candidates
        LinkedList<Node<T>> initial_candidates = new LinkedList<Node<T>>();
        initial_candidates.add(node_to_remove);
        initial_candidates.addAll(nodes_to_update);

        // In spark 1.6.0 the list returned by collect causes an
        // UnsupportedOperationException when you try to remove :(
        LinkedList<Node<T>> candidates  = new LinkedList<Node<T>>(
                        distributed_graph
                        .flatMap(new SearchNeighbors(
                                initial_candidates,
                                update_depth))
                        .collect());

        // Find the partition corresponding to node_to_remove
        // The balanced kmedoids partitioner wrote this information in the
        // attributes of the node, in the distributed graph
        // hence not necessarily in node_to_remove provided as parameter...
        // This is dirty :(
        int partition_of_node_to_remove = 0;
        for (Node<T> node : candidates) {
            if (node.equals(node_to_remove)) {
                if (null != node.getAttribute(
                                NodePartitioner.PARTITION_KEY)) {
                    partition_of_node_to_remove =
                            (Integer) node.getAttribute(
                                NodePartitioner.PARTITION_KEY);
                    break;
                }
            }
        }

        while (candidates.contains(node_to_remove)) {
            candidates.remove(node_to_remove);
        }

        // Update the graph and remove the node
        JavaRDD<Graph<T>> updated_graph = distributed_graph
                .map(new RemoveUpdate(
                        node_to_remove,
                        nodes_to_update,
                        candidates,
                        stats_accumulator))
                .cache();

        // bookkeeping: update the counts
        partitions_size[partition_of_node_to_remove]--;

        //  truncate RDD DAG (would cause a stack overflow, even with caching)
        if ((nodes_added_or_removed % ITERATIONS_BEFORE_CHECKPOINT) == 0) {
            updated_graph.rdd().localCheckpoint();
        }

        // Keep a track of updated RDD to unpersist after two iterations
        previous_rdds.add(updated_graph);
        if (nodes_added_or_removed > 2) {
            previous_rdds.pop().unpersist();
        }

        nodes_added_or_removed++;

        // Force execution and use updated graph
        updated_graph.count();
        distributed_graph = updated_graph;

    }

    /**
     * Get the current graph, represented as a RDD of Graph.
     * @return the current graph
     */
    public final JavaRDD<Graph<T>> getDistributedGraph() {
        return distributed_graph;
    }

    /**
     * Get the current graph, converted to a RDD of Tuples (Node, NeighborList).
     * @return
     */
    public final JavaPairRDD<Node<T>, NeighborList> getGraph() {
        return distributed_graph.flatMapToPair(new MergeGraphs());
    }

    private long[] getPartitionsSize(final JavaRDD<Graph<T>> graph) {

        List<Long> counts_list = graph.map(
                new SubgraphSizeFunction()).collect();

        long[] result = new long[counts_list.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = counts_list.get(i);
        }
        return result;
    }

    /**
     * Compute the number of nodes that can be added before we recompute the
     * medoids (depends on current size and medoid_update_ratio).
     */
    private long computeNodesBeforeUpdate() {
        if (medoid_update_ratio == 0.0) {
            return Long.MAX_VALUE;
        }

        return (long) (getSize() * medoid_update_ratio);
    }
}

/**
 * Used to count the number of nodes in each partition, when we initialize the
 * distributed online graph. Returns the size of each subgraph.
 * @author Thibault Debatty
 * @param <T>
 */
class SubgraphSizeFunction<T> implements Function<Graph<T>, Long> {

    public Long call(final Graph<T> subgraph) {
        return new Long(subgraph.size());
    }
}

/**
 *
 * @author Thibault Debatty
 * @param <T>
 */
class AddNode<T> implements Function<Graph<T>, Graph<T>> {
    private final Node<T> node;
    private final NeighborList neighborlist;

    AddNode(final Node<T> node, final NeighborList neighborlist) {
        this.node = node;
        this.neighborlist = neighborlist;
    }

    public Graph<T> call(final Graph<T> graph) {
        Node<T> one_node = graph.getNodes().iterator().next();

        if (node.getAttribute(NodePartitioner.PARTITION_KEY).equals(
                one_node.getAttribute(
                        NodePartitioner.PARTITION_KEY))) {
            graph.put(node, neighborlist);
        }

        return graph;
    }
}

/**
 * Update the graph when adding a node.
 * @author Thibault Debatty
 * @param <T>
 */
class UpdateFunction<T>
        implements Function<Graph<T>, Graph<T>> {

    private final int update_depth;
    private final NeighborList neighborlist;
    private final SimilarityInterface<T> similarity;
    private final Node<T> node;
    private final StatisticsAccumulator stats_accumulator;

    UpdateFunction(
            final Node<T> node,
            final NeighborList neighborlist,
            final SimilarityInterface<T> similarity,
            final StatisticsAccumulator stats_accumulator,
            final int update_depth) {

        this.node = node;
        this.neighborlist = neighborlist;
        this.similarity = similarity;
        this.stats_accumulator = stats_accumulator;
        this.update_depth = update_depth;
    }

    public Graph<T> call(final Graph<T> local_graph) {

        // Nodes to analyze at this iteration
        LinkedList<Node<T>> analyze = new LinkedList<Node<T>>();

        // Nodes to analyze at next iteration
        LinkedList<Node<T>> next_analyze = new LinkedList<Node<T>>();

        // List of already analyzed nodes
        HashMap<Node<T>, Boolean> visited = new HashMap<Node<T>, Boolean>();

        // Fill the list of nodes to analyze
        for (Neighbor neighbor : neighborlist) {
            analyze.add(neighbor.node);
        }

        StatisticsContainer local_stats = new StatisticsContainer();

        for (int depth = 0; depth < update_depth; depth++) {
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
                                (T) other.value)));

                local_stats.incAddSimilarities();

                visited.put(other, Boolean.TRUE);
            }

            analyze = next_analyze;
            next_analyze = new LinkedList<Node<T>>();
        }

        if (stats_accumulator != null) {
            stats_accumulator.add(local_stats);
        }

        return local_graph;
    }
}

/**
 * In this Spark implementation, the distributed graph is stored as a RDD of
 * subgraphs, this function collects the subgraphs and returns a single graph,
 * represented as an RDD of tuples (Node, NeighborList).
 * This function is used by the method Online.getGraph().
 * @author Thibault Debatty
 * @param <T>
 */
class MergeGraphs<T>
    implements PairFlatMapFunction<Graph<T>, Node<T>, NeighborList> {

    public Iterator<Tuple2<Node<T>, NeighborList>> call(final Graph<T> graph) {

        ArrayList<Tuple2<Node<T>, NeighborList>> list =
                new ArrayList<Tuple2<Node<T>, NeighborList>>(graph.size());

        for (Map.Entry<Node<T>, NeighborList> entry : graph.entrySet()) {
            list.add(new Tuple2<Node<T>, NeighborList>(
                    entry.getKey(),
                    entry.getValue()));
        }

        return list.iterator();
    }
}

/**
 * Used by fastRemove to find the nodes that should be updated.
 * @author Thibault Debatty
 * @param <T>
 */
class FindNodesToUpdate<T> implements FlatMapFunction<Graph<T>, Node<T>> {
    private final Node<T> node_to_remove;

    FindNodesToUpdate(final Node<T> node_to_remove) {
        this.node_to_remove = node_to_remove;
    }

    public Iterator<Node<T>> call(final Graph<T> subgraph) {
        LinkedList<Node<T>> nodes_to_update = new LinkedList<Node<T>>();
        for (Node<T> node : subgraph.getNodes()) {
            if (subgraph.get(node).containsNode(node_to_remove)) {
                nodes_to_update.add(node);
            }
        }

        return nodes_to_update.iterator();
    }
}

/**
 * Search neighbors from a list of starting points, up to a fixed depth.
 * Used in Online.fastRemove(node) to search the candidates.
 * @author Thibault Debatty
 * @param <T>
 */
class SearchNeighbors<T> implements FlatMapFunction<Graph<T>, Node<T>> {
    private final int search_depth;

    private final LinkedList<Node<T>> starting_points;

    SearchNeighbors(
            final LinkedList<Node<T>> initial_candidates,
            final int search_depth) {

        this.starting_points = initial_candidates;
        this.search_depth = search_depth;
    }

    public Iterator<Node<T>> call(final Graph<T> subgraph) {
        return subgraph.findNeighbors(starting_points, search_depth).iterator();
    }
}

/**
 * When removing a node, update the subgraphs: remove the node, and assign
 * a new neighbor to nodes that had this node as neighbor.
 * @author Thibault Debatty
 * @param <T>
 */
class RemoveUpdate<T> implements Function<Graph<T>, Graph<T>> {
    private final Node<T> node_to_remove;
    private final List<Node<T>> nodes_to_update;
    private final List<Node<T>> candidates;
    private final StatisticsAccumulator stats_accumulator;

    RemoveUpdate(
            final Node<T> node_to_remove,
            final List<Node<T>> nodes_to_update,
            final List<Node<T>> candidates,
            final StatisticsAccumulator stats_accumulator) {

        this.node_to_remove = node_to_remove;
        this.nodes_to_update = nodes_to_update;
        this.candidates = candidates;
        this.stats_accumulator = stats_accumulator;

    }

    public Graph<T> call(final Graph<T> subgraph) {

        StatisticsContainer local_stats = new StatisticsContainer();

        // Remove the node (if present in this subgraph)
        subgraph.getHashMap().remove(node_to_remove);

        for (Node<T> node_to_update : nodes_to_update) {
            if (!subgraph.containsKey(node_to_update)) {
                // This node belongs to another subgraph => skip
                continue;
            }

            NeighborList nl_to_update = subgraph.get(node_to_update);

            // Remove the old node
            nl_to_update.removeNode(node_to_remove);

            // Replace the old node by the best candidate
            for (Node<T> candidate : candidates) {
                if (candidate.equals(node_to_update)) {
                    continue;
                }

                double similarity = subgraph.getSimilarity().similarity(
                        node_to_update.value,
                        candidate.value);
                local_stats.incRemoveSimilarities();

                nl_to_update.add(new Neighbor(candidate, similarity));
            }
        }

        stats_accumulator.add(local_stats);
        return subgraph;
    }
}

/**
 * Used to update medoids after nodes are added to the graph. The input is a
 * RDD of Graph.
 * @author Thibault Debatty
 * @param <T>
 */
/*
class ComputeMedoids<T> implements Function<Graph<T>, Node<T>> {

    public Node<T> call(final Graph<T> graph) {
        if (graph.size() == 0) {
            return null;
        }

        // This partition might contain multiple subgraphs
        // => find largest subgraph
        ArrayList<Graph<T>> strongly_connected_components
                = graph.stronglyConnectedComponents();
        int largest_subgraph_size = 0;
        Graph<T> largest_subgraph = strongly_connected_components.get(0);
        for (Graph<T> subgraph : strongly_connected_components) {
            if (subgraph.size() > largest_subgraph_size) {
                largest_subgraph = subgraph;
                largest_subgraph_size = subgraph.size();
            }
        }

        int largest_distance = Integer.MAX_VALUE;
        Node medoid = (Node) largest_subgraph.getNodes().iterator().next();
        for (Node n : largest_subgraph.getNodes()) {
            //Node n = (Node) o;
            Dijkstra dijkstra = new Dijkstra(largest_subgraph, n);

            int node_largest_distance = dijkstra.getLargestDistance();

            if (node_largest_distance == 0) {
                continue;
            }

            if (node_largest_distance < largest_distance) {
                largest_distance = node_largest_distance;
                medoid = n;
            }
        }

        return medoid;
    }
}*/
