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
import info.debatty.spark.knngraphs.BalancedKMedoidsPartitioner;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.spark.Accumulator;
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

    /**
     * Key used to store the sequence number of the nodes. Used by the window
     * algorithm to remove the nodes.
     */
    public static final String NODE_SEQUENCE_KEY = "ONLINE_SEQ_KEY";

    private static final int DEFAULT_PARTITIONING_ITERATIONS = 5;
    private static final int DEFAULT_UPDATE_DEPTH = 2;
    private static final double DEFAULT_MEDOID_UPDATE_RATIO = 0.1;

    // Number of nodes to add before performing a checkpoint
    // (to strip RDD DAG)
    private static final int ITERATIONS_BETWEEN_CHECKPOINTS = 100;

    // the search algorithm also contains a reference to the current graph
    private final ApproximateSearch<T> searcher;
    private final int k;
    private final SimilarityInterface<T> similarity;
    // Number of nodes to add before recomputing centroids
    private double medoid_update_ratio = DEFAULT_MEDOID_UPDATE_RATIO;

    private final long[] partitions_size;
    private final LinkedList<JavaRDD<Graph<T>>> previous_rdds;

    private double search_speedup = ApproximateSearch.DEFAULT_SPEEDUP;
    private int search_random_jumps = ApproximateSearch.DEFAULT_JUMPS;
    private double search_expansion = ApproximateSearch.DEFAULT_EXPANSION;

    private long nodes_added_or_removed;
    private long nodes_before_update_medoids;
    private int window_size = 0;
    private final JavaSparkContext spark_context;
    private int update_depth = DEFAULT_UPDATE_DEPTH;

    /**
     *
     * @param k number of edges per node
     * @param similarity similarity to use for computing edges
     * @param sc spark context
     * @param initial_graph initial graph
     * @param partitioning_medoids number of partitions
     */
    public Online(
            final int k,
            final SimilarityInterface<T> similarity,
            final JavaSparkContext sc,
            final JavaPairRDD<Node<T>, NeighborList> initial_graph,
            final int partitioning_medoids) {

        this(
                k,
                similarity,
                sc,
                initial_graph,
                partitioning_medoids,
                DEFAULT_PARTITIONING_ITERATIONS);
    }

    /**
     *
     * @param k
     * @param similarity
     * @param sc
     * @param initial_graph
     * @param partitioning_medoids
     * @param partitioning_iterations
     */
    public Online(
            final int k,
            final SimilarityInterface<T> similarity,
            final JavaSparkContext sc,
            final JavaPairRDD<Node<T>, NeighborList> initial_graph,
            final int partitioning_medoids,
            final int partitioning_iterations) {

        this.nodes_added_or_removed = 0;
        this.similarity = similarity;
        this.k = k;

        // Use the distributed search algorithm to partition the graph
        this.searcher = new ApproximateSearch<T>(
                initial_graph,
                partitioning_iterations,
                partitioning_medoids,
                similarity);

        this.spark_context = sc;

        sc.setCheckpointDir("/tmp/checkpoints");

        this.partitions_size = getPartitionsSize(searcher.getGraph());
        this.previous_rdds = new LinkedList<JavaRDD<Graph<T>>>();
        this.nodes_before_update_medoids = computeNodesBeforeUpdate();

    }

    /**
     * Set the update depth for fast adding or removing nodes (default is 2).
     * @param update_depth
     */
    public final void setUpdateDepth(final int update_depth) {
        this.update_depth = update_depth;
    }

    /**
     * Get the size of the window.
     * @return
     */
    public final int getWindowSize() {
        return window_size;
    }

    /**
     * Set the size of the window (for removing a point when a new point is
     * added to graph).
     * @param window_size
     */
    public final void setWindowSize(final int window_size) {
        this.window_size = window_size;
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
     * Set the speedup of the search step to add a node (default: 4).
     * @param search_speedup speedup
     */
    public final void setSearchSpeedup(final double search_speedup) {
        this.search_speedup = search_speedup;
    }

    /**
     *
     * @param search_random_jumps
     */
    public final void setSearchRandomJumps(final int search_random_jumps) {
        this.search_random_jumps = search_random_jumps;
    }

    /**
     *
     * @param search_expansion
     */
    public final void setSearchExpansion(final double search_expansion) {
        this.search_expansion = search_expansion;
    }

    /**
     * Set the ratio of nodes to add to the graph before recomputing the
     * medoids (default: 0.1).
     * @param update_ratio [0.0 ...] (0 = disable medoid update)
     */
    public final void setMedoidUpdateRatio(final double update_ratio) {
        if (update_ratio < 0) {
            throw new InvalidParameterException("Update ratio must be >= 0!");
        }
        this.medoid_update_ratio = update_ratio;
        this.nodes_before_update_medoids = computeNodesBeforeUpdate();
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
            final Accumulator<StatisticsContainer> stats_accumulator) {


        if (window_size != 0) {
            // TODO: add stats_accumulator in fast remove!
            fastRemove(
                (Integer) node.getAttribute(NODE_SEQUENCE_KEY) - window_size);
        }

        // Find the neighbors of this node
        NeighborList neighborlist = searcher.search(
                node,
                k,
                search_speedup,
                search_random_jumps,
                search_expansion,
                stats_accumulator);

        // Assign the node to a partition (most similar medoid, with partition
        // size constraint)
        searcher.assign(node, partitions_size);
        //similarities += k;

        // bookkeeping: update the counts
        partitions_size[(Integer) node
                .getAttribute(BalancedKMedoidsPartitioner.PARTITION_KEY)]++;
        // update the existing graph edges

        JavaRDD<Graph<T>> updated_graph = searcher.getGraph().map(
                    new UpdateFunction<T>(
                            node,
                            neighborlist,
                            similarity,
                            stats_accumulator,
                            update_depth));

        // Add the new <Node, NeighborList> to the distributed graph
        updated_graph = updated_graph.map(new AddNode(node, neighborlist));

        //  truncate RDD DAG (would cause a stack overflow, even with caching)
        if ((nodes_added_or_removed % ITERATIONS_BETWEEN_CHECKPOINTS) == 0) {
            updated_graph.checkpoint();
        }

        // Keep a track of updated RDD to unpersist after two iterations
        previous_rdds.add(updated_graph);
        if (nodes_added_or_removed > 2) {
            previous_rdds.pop().unpersist();
        }

        // Force execution
        updated_graph.count();

        // From now on use the new graph...
        searcher.setGraph(updated_graph);

        nodes_added_or_removed++;
        nodes_before_update_medoids--;
        if (nodes_before_update_medoids == 0) {
            // TODO: count number of computed similarities here!!
            searcher.getPartitioner().computeNewMedoids(updated_graph);
            nodes_before_update_medoids = computeNodesBeforeUpdate();
        }
    }

    /**
     * Remove a node using fast approximate algorithm.
     * @param node_to_remove
     * @return number of computed similarities
     */
    public final int fastRemove(final Node<T> node_to_remove) {
        // find the list of nodes to update
        List<Node<T>> nodes_to_update = searcher.getGraph()
                .flatMap(new FindNodesToUpdate(node_to_remove))
                .collect();

        // build the list of candidates
        LinkedList<Node<T>> initial_candidates = new LinkedList<Node<T>>();
        initial_candidates.add(node_to_remove);
        initial_candidates.addAll(nodes_to_update);

        // In spark 1.6.0 the list returned by collect causes an
        // UnsupportedOperationException when you try to remove :(
        LinkedList<Node<T>> candidates  =
                new LinkedList<Node<T>>(
                        searcher.getGraph()
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
                                BalancedKMedoidsPartitioner.PARTITION_KEY)) {
                    partition_of_node_to_remove =
                            (Integer) node.getAttribute(
                                BalancedKMedoidsPartitioner.PARTITION_KEY);
                    break;
                }
            }
        }

        while (candidates.contains(node_to_remove)) {
            candidates.remove(node_to_remove);
        }

        // Update the graph and remove the node
        Accumulator<Integer> similarities_accumulator
                = spark_context.accumulator(0);
        JavaRDD<Graph<T>> updated_graph = searcher.getGraph()
                .map(new RemoveUpdate(
                        node_to_remove,
                        nodes_to_update,
                        candidates,
                        similarities_accumulator))
                .cache();

        // bookkeeping: update the counts
        partitions_size[partition_of_node_to_remove]--;

        //  truncate RDD DAG (would cause a stack overflow, even with caching)
        if ((nodes_added_or_removed % ITERATIONS_BETWEEN_CHECKPOINTS) == 0) {
            updated_graph.checkpoint();
        }

        // Keep a track of updated RDD to unpersist after two iterations
        previous_rdds.add(updated_graph);
        if (nodes_added_or_removed > 2) {
            previous_rdds.pop().unpersist();
        }

        nodes_added_or_removed++;

        // Force execution and use updated graph
        updated_graph.count();
        searcher.setGraph(updated_graph);

        return similarities_accumulator.value();
    }

    /**
     * Remove a node using the node sequence number instead of the node itself.
     * Used by the sliding window algorithm.
     * @param node_sequence
     */
    private int fastRemove(final long node_sequence) {
        // This is not really efficient :(
        List<Node<T>> nodes = searcher.getGraph()
                .flatMap(new FindNode(node_sequence))
                .collect();

        if (nodes.isEmpty()) {
            System.out.println("Node sequence not found: " + node_sequence);
            return 0;
        }

        return fastRemove(nodes.get(0));
    }

    /**
     * Get the current graph, represented as a RDD of Graph.
     * @return the current graph
     */
    public final JavaRDD<Graph<T>> getDistributedGraph() {
        return searcher.getGraph();
    }

    /**
     * Get the current graph, converted to a RDD of Tuples (Node, NeighborList).
     * @return
     */
    public final JavaPairRDD<Node<T>, NeighborList> getGraph() {
        return searcher.getGraph().flatMapToPair(new MergeGraphs());
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

        if (node.getAttribute(BalancedKMedoidsPartitioner.PARTITION_KEY).equals(
                one_node.getAttribute(
                        BalancedKMedoidsPartitioner.PARTITION_KEY))) {
            graph.put(node, neighborlist);
        }

        return graph;
    }
}

/**
 * Used to find the node corresponding to a given sequence number.
 * @author Thibault Debatty
 * @param <T>
 */
class FindNode<T> implements FlatMapFunction<Graph<T>, Node<T>> {
    private final long sequence_number;

    FindNode(final long sequence_number) {
        this.sequence_number = sequence_number;
    }

    public Iterable<Node<T>> call(final Graph<T> subgraph) {

        LinkedList<Node<T>> result = new LinkedList<Node<T>>();
        for (Node<T> node : subgraph.getNodes()) {
            Integer node_sequence = (Integer) node.getAttribute(
                    Online.NODE_SEQUENCE_KEY);
            if (node_sequence == sequence_number) {
                result.add(node);
                return result;
            }
        }
        return result;
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
    private final Accumulator<StatisticsContainer> stats_accumulator;

    UpdateFunction(
            final Node<T> node,
            final NeighborList neighborlist,
            final SimilarityInterface<T> similarity,
            final Accumulator<StatisticsContainer> stats_accumulator,
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

    public Iterable<Tuple2<Node<T>, NeighborList>> call(final Graph<T> graph) {

        ArrayList<Tuple2<Node<T>, NeighborList>> list =
                new ArrayList<Tuple2<Node<T>, NeighborList>>(graph.size());

        for (Map.Entry<Node<T>, NeighborList> entry : graph.entrySet()) {
            list.add(new Tuple2<Node<T>, NeighborList>(
                    entry.getKey(),
                    entry.getValue()));
        }

        return list;
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

    public Iterable<Node<T>> call(final Graph<T> subgraph) {
        LinkedList<Node<T>> nodes_to_update = new LinkedList<Node<T>>();
        for (Node<T> node : subgraph.getNodes()) {
            if (subgraph.get(node).containsNode(node_to_remove)) {
                nodes_to_update.add(node);
            }
        }

        return nodes_to_update;
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

    public Iterable<Node<T>> call(final Graph<T> subgraph) {
        return subgraph.findNeighbors(starting_points, search_depth);
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
    private final Accumulator<Integer> similarities_accumulator;

    RemoveUpdate(
            final Node<T> node_to_remove,
            final List<Node<T>> nodes_to_update,
            final List<Node<T>> candidates,
            final Accumulator<Integer> similarities_accumulator) {

        this.node_to_remove = node_to_remove;
        this.nodes_to_update = nodes_to_update;
        this.candidates = candidates;
        this.similarities_accumulator = similarities_accumulator;

    }

    public Graph<T> call(final Graph<T> subgraph) {

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
                similarities_accumulator.add(1);

                nl_to_update.add(new Neighbor(candidate, similarity));
            }
        }

        return subgraph;
    }
}