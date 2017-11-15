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
import static com.google.common.primitives.Ints.max;
import info.debatty.java.graphs.Graph;
import info.debatty.java.graphs.Neighbor;
import info.debatty.java.graphs.NeighborList;
import info.debatty.spark.knngraphs.DistributedGraph;
import info.debatty.spark.knngraphs.partitioner.jabeja.Budget;
import info.debatty.spark.knngraphs.partitioner.jabeja.UnlimitedBudget;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 *
 * @author Thibault Debatty
 * @param <T> type of data to process
 */
public class JaBeJa<T> implements Partitioner<T> {

    private static final double T0 = 2.0;
    private static final double DELTA = 0.003;
    private static final int SWAPS_PER_ITERATION = 10;
    private static final int ITERATIONS_BEFORE_CHECKPOINT = 20;
    private static final int RDDS_TO_CACHE = 3;

    private static final Logger LOGGER = LoggerFactory.getLogger(JaBeJa.class);

    private final int partitions;
    private final Budget budget;









    /**
     *
     * @param partitions
     * @param budget
     */
    public JaBeJa(final int partitions, final Budget budget) {
        this.partitions = partitions;
        this.budget = budget;
    }

    public JaBeJa(final int partitions) {
        this.partitions = partitions;
        this.budget = new UnlimitedBudget();
    }

    /**
     *
     * @param input_graph
     * @return
     */
    public final Partitioning<T> partition(
            final JavaPairRDD<T, NeighborList> input_graph) {

        Partitioning<T> solution = new Partitioning<T>();
        LinkedList<JavaPairRDD<T, NeighborList>> previous_rdds =
                new LinkedList<JavaPairRDD<T, NeighborList>>();

        // Randomize
        solution.graph = randomize(input_graph);
        solution.graph = DistributedGraph.moveNodes(solution.graph, partitions);
        solution.graph.cache();
        solution.graph.count();

        // Perform swaps
        double tr = T0;
        int iteration = 0;
        while (true) {
            iteration++;
            LOGGER.info("Tr = {}", tr);

            // Perform swap
            SwapResult<T> swap_result = swap(
                    solution.graph, tr, SWAPS_PER_ITERATION);
            LOGGER.info("Performed {} swaps", swap_result.swaps);
            solution.graph = swap_result.graph;
            solution.graph.cache();

            if ((iteration % ITERATIONS_BEFORE_CHECKPOINT) == 0) {
                LOGGER.info("Checkpoint");
                solution.graph.rdd().localCheckpoint();
            }

            // Force execution
            solution.graph.count();

            // Keep a track of updated RDD to unpersist after two iterations
            previous_rdds.add(solution.graph);
            if (iteration > RDDS_TO_CACHE) {
                previous_rdds.pop().unpersist();
            }

            if (swap_result.swaps <= 0) {
                break;
            }

            if (budget.isExhausted(solution)) {
                break;
            }

            tr = Math.max(1.0, tr - DELTA);
        }

        solution.graph = DistributedGraph.moveNodes(solution.graph, partitions);
        solution.graph.cache();
        solution.graph.count();
        solution.end_time = System.currentTimeMillis();
        return solution;
    }

    /**
     *
     * @param <U>
     * @param graph
     * @param partitions
     * @return
     */
    public static final <U> int countCrossEdges(
            final JavaPairRDD<Node<U>, NeighborList> graph,
            final int partitions) {

        Map<Long, Integer> color_index = buildColorIndex(graph);
        int[][] degrees_index = buildDegreesIndex(
                graph, color_index, partitions);

        return countCrossEdges(color_index, degrees_index);
    }

    /**
     * Compute the imbalance between partitions.
     * max / sum * partitions
     * @param <U>
     * @param graph
     * @param partitions
     * @return
     */
    public static final <U> double computeBalance(
            final JavaPairRDD<Node<U>, NeighborList> graph,
            final int partitions) {

        Map<Long, Integer> color_index = buildColorIndex(graph);
        return computeBalance(color_index, partitions);
    }


    final JavaPairRDD<T, NeighborList> randomize(
            final JavaPairRDD<T, NeighborList> input_graph) {

        return input_graph.mapToPair(new RandomizeFunction<T>(partitions));
    }

    /**
     * Build the index that holds the color (partition) corresponding to each
     * node.
     *
     * @param <U>
     * @param graph
     * @return
     */
    public static final <U> Map<Long, Integer> buildColorIndex(
            final JavaPairRDD<Node<U>, NeighborList> graph) {

        return graph.mapToPair(new GetPartitionFunction<U>()).collectAsMap();
    }

    /**
     * Compute the imbalance = size of biggest partition / average partition
     * size.
     *
     * @param <U>
     * @param color_index
     * @param partitions
     * @return
     */
    public static final <U> double computeBalance(
            final Map<Long, Integer> color_index, final int partitions) {

        int[] sizes = new int[partitions];
        for (int color : color_index.values()) {
            sizes[color]++;
        }

        LOGGER.info("Sizes: {}", sizes);
        int max = max(sizes);
        int sum = sum(sizes);

        double imbalance = 1.0 * max / sum * partitions;
        return imbalance;
    }

    private static int sum(final int[] values) {
        int sum = 0;
        for (int value : values) {
            sum += value;
        }

        return sum;
    }

    static final <U> Map<Integer, int[]> buildDegreesIndex(
            final JavaPairRDD<U, NeighborList> graph,
            final Map<Long, Integer> color_index,
            final int partitions) {

        return graph
                .mapToPair(new GetDegreesFunction<U>(partitions, color_index))
                .collectAsMap();
    }

    /**
     * Perform a single swap in each partition.
     * This method will not physically move the nodes, you must use moveNodes()
     * therefore. It will modify the value of
     * node.getValue(NodePartitionner.KEY).
     *
     * The swap takes 3 steps:
     * 1. src node sends swap request
     * 2. dst node receives request + sends ACK to confirm
     * 3. src + dst receive ACK and update
     *
     * @param graph
     * @param index
     */
    final SwapResult<T> swap(
            final JavaPairRDD<T, NeighborList> graph,
            final double tr,
            final int swaps_per_iteration) {

        int[] color_index = buildColorIndex(graph);
        int[][] degrees_index = buildDegreesIndex(
                graph, color_index, partitions);

        LOGGER.info(
                "Cross edges: {}", countCrossEdges(color_index, degrees_index));

        LOGGER.info("Imbalance: {}", computeBalance(color_index, partitions));

        List<SwapRequest> requests = graph
                .mapPartitions(
                        new MakeRequestsFunction<T>(
                                color_index, degrees_index,
                                tr,
                                swaps_per_iteration))
                .collect();
        LOGGER.info("Swaps: {}", requests.size());
        LOGGER.debug("{}", requests);

        List<SwapRequest<T>> acks = graph
                .mapPartitions(
                    new ProcessRequestsFunction<T>(requests))
                .collect();

        LOGGER.debug("{}", acks);
        assert acks.size() <= requests.size();

        // Perform the swap
        JavaPairRDD<T, NeighborList> partitioned_graph =
                graph.mapPartitionsToPair(new PerformSwapFunction<T>(acks));

        return new SwapResult(
                partitioned_graph,
                acks.size());
    }

    private static int countCrossEdges(
            final int[] color_index, final int[][] degrees_index) {
        int partitions = degrees_index[0].length;
        int count = 0;
        for (int i = 0; i < color_index.length; i++) {
            int color = color_index[i];
            for (int j = 0; j < partitions; j++) {
                if (j != color) {
                    count += degrees_index[i][j];
                }
            }
        }

        return count;
    }
}

/**
 * Perform the actual swap, using validated swap requests.
 * @author Thibault Debatty
 * @param <T>
 */
class PerformSwapFunction<T>
        implements PairFlatMapFunction<
            Iterator<Tuple2<T, NeighborList>>,
            T,
            NeighborList> {

    private final Logger logger =
            LoggerFactory.getLogger(PerformSwapFunction.class);

    private final List<SwapRequest<T>> acks;

    PerformSwapFunction(final List<SwapRequest<T>> acks) {
        this.acks = acks;
    }

    public Iterator<Tuple2<T, NeighborList>> call(
            final Iterator<Tuple2<T, NeighborList>> tuples_iterator) {

        // Collect all nodes
        HashMap<String, T> index = new HashMap<>();
        List<Tuple2<T, NeighborList>> tuples = new LinkedList<>();

        while (tuples_iterator.hasNext()) {
            Tuple2<T, NeighborList> tuple  = tuples_iterator.next();
            index.put(tuple._1.id, tuple._1);
            tuples.add(tuple);
        }

        logger.debug("#tuples: {}", tuples.size());

        // Process the requests
        for (SwapRequest<T> request : acks) {
            T local_src_node = index.get(request.src.id);
            if (local_src_node != null) {
                logger.debug(
                        "Moving node {} to partition {}",
                        local_src_node.id,
                        request.dst_color);
                local_src_node.setAttribute(
                        NodePartitioner.PARTITION_KEY,
                        request.dst_color);
            }

            T local_dst_node = index.get(request.dst.id);
            if (local_dst_node != null) {
                logger.debug(
                        "Moving node {} to partition {}",
                        local_dst_node.id,
                        request.src_color);
                local_dst_node.setAttribute(
                        NodePartitioner.PARTITION_KEY,
                        request.src_color);
            }
        }

        return tuples.iterator();
    }
}

/**
 * Check requests, to avoid same nodes is swapped twice during the same
 * iteration.
 * @author Thibault Debatty
 * @param <T>
 */
class ProcessRequestsFunction<T>
        implements FlatMapFunction<
            Iterator<Tuple2<T, NeighborList>>,
            SwapRequest<T>> {

    private final List<SwapRequest> swap_requests;
    private final Logger logger =
            LoggerFactory.getLogger(ProcessRequestsFunction.class);

    ProcessRequestsFunction(final List<SwapRequest> swap_requests) {
        this.swap_requests = swap_requests;
    }

    public Iterator<SwapRequest<T>> call(
            final Iterator<Tuple2<T, NeighborList>> tuples) {

        // Collect all nodes
        Graph<T> local_graph = new Graph<T>();
        while (tuples.hasNext()) {
            Tuple2<T, NeighborList> tuple  = tuples.next();
            local_graph.put(tuple._1, tuple._2);
        }

        List<String> nodes_will_swap = new LinkedList<String>();
        List<SwapRequest<T>> accepted_requests =
                new LinkedList<SwapRequest<T>>();

        for (SwapRequest request : swap_requests) {
            nodes_will_swap.add(request.src.id);
        }

        for (SwapRequest request : swap_requests) {
            if (local_graph.get(request.dst) == null) {
                continue;
            }

            logger.debug("Found a request for me");

            if (nodes_will_swap.contains(request.dst.id)) {
                logger.debug("This dst node already received a swap request");
                continue;
            }
            nodes_will_swap.add(request.dst.id);
            accepted_requests.add(request);
        }
        return accepted_requests.iterator();

    }

}

/**
 * Create swap requests, using JaBeJa procedure.
 * @author Thibault Debatty
 * @param <T>
 */
class MakeRequestsFunction<T>
        implements FlatMapFunction<
            Iterator<Tuple2<T, NeighborList>>,
            SwapRequest> {

    private static final int SAMPLE_COUNT = 10;
    private static final double ALPHA = 2.0;

    private final Logger logger =
            LoggerFactory.getLogger(MakeRequestsFunction.class);

    /**
     * Indicates the color (partition) of each node.
     */
    private final int[] color_index;
    private final int[][] degrees_index;
    private Graph<T> local_graph;
    private final double tr;
    private final int swaps_per_iteration;

    MakeRequestsFunction(
            final int[] color_index,
            final int[][] degrees_index,
            final double tr,
            final int swaps_per_iteration) {

        this.color_index = color_index;
        this.degrees_index = degrees_index;
        this.tr = tr;
        this.swaps_per_iteration = swaps_per_iteration;
    }

    public Iterator<SwapRequest> call(
            final Iterator<Tuple2<T, NeighborList>> tuples)
            throws Exception {

        // Collect all nodes
        local_graph = new Graph<T>();
        while (tuples.hasNext()) {
            Tuple2<T, NeighborList> tuple  = tuples.next();
            local_graph.put(tuple._1, tuple._2);
        }

        LinkedList<String> nodes_will_swap = new LinkedList<String>();
        LinkedList<SwapRequest> list = new LinkedList<SwapRequest>();
        for (int i = 0; i < swaps_per_iteration; i++) {

            // Select one node at random
            T src = pickRandomNode(local_graph);

            if (nodes_will_swap.contains(src.id)) {
                continue;
            }

            nodes_will_swap.add(src.id);

            // Perform JaBeJa:
            // partner = findPartner(p.getNeighbors(), Tr)
            T dst = findPartner(
                    src, getNodes(local_graph.getNeighbors(src)), tr);

            // if (partner == null) : partner = findPartner(p.getSample, Tr)
            if (dst == null) {
                dst = findPartner(
                        src, pickRandomNodes(local_graph, SAMPLE_COUNT), tr);
            }

            // if (partner not null) : perform swap handshake
            if (dst != null) {
                logger.debug("Found a partner for swap");
                list.add(
                        new SwapRequest(
                                src, dst, getColor(src), getColor(dst)));

            }
        }
        return list.iterator();

    }

    private T pickRandomNode(final Graph<T> graph) {
        return pickRandomNodes(graph, 1).get(0);
    }

    private List<T> pickRandomNodes(
            final Graph<T> graph, final int count) {
        List<T> list = new LinkedList<T>();

        Set<T> nodes = graph.getHashMap().keySet();
        int n = nodes.size();
        Random rand = new Random();

        while (list.size() < count) {
            int position = rand.nextInt(n);
            Iterator<T> iterator = nodes.iterator();

            T node = null;
            for (int j = 0; j <= position; j++) {
                node = iterator.next();
            }

            if (!list.contains(node)) {
                list.add(node);
            }
        }

        return list;
    }

    private T findPartner(
            final T p, final List<T> nodes, final double tr) {
        logger.debug("Search a partner for swap");
        double highest_score = 0;
        T partner = null;

        for (T q : nodes) {
            int p_color = getColor(p);
            int q_color = getColor(q);
            int dpp = getDegree(p, p_color);
            int dqq = getDegree(q, q_color);
            double old_score = Math.pow(dpp, ALPHA) + Math.pow(dqq, ALPHA);

            int dpq = getDegree(p, q_color);
            int dqp = getDegree(q, p_color);
            double new_score = Math.pow(dpq, ALPHA) + Math.pow(dqp, ALPHA);

            if (new_score * tr > old_score
                    && new_score > highest_score) {

                logger.debug("Found new partner");
                partner = q;
                highest_score = new_score;
            }
        }

        return partner;
    }

    /**
     * Extract the list of nodes from a neighborlist.
     * @param get
     * @return
     */
    private List<T> getNodes(final NeighborList neighborlist) {
        LinkedList<T> nodes = new LinkedList<>();
        for (Neighbor<T> neighbor : neighborlist) {
            nodes.add(neighbor.getNode());
        }
        return nodes;
    }

    private int getColor(final T node) {
        return color_index[Integer.valueOf(node.id)];
    }

    /**
     * Computes the number of neighbors of this node that have the specified
     * color.
     * @param node
     * @param color
     * @return
     */
    private int getDegree(final T node, final int color) {
        return degrees_index[Integer.valueOf(node.id)][color];

    }
}

/**
 * Compute the degrees of each node, using the color index.
 * @author Thibault Debatty
 * @param <T>
 */
class GetDegreesFunction<T>
    implements PairFunction<Tuple2<T, NeighborList>, Integer, int[]> {

    private final int partitions;
    private final Map<Long, Integer> color_index;

    GetDegreesFunction(
            final int partitions, final Map<Long, Integer> color_index) {
        this.partitions = partitions;
        this.color_index = color_index;
    }

    public Tuple2<Integer, int[]> call(
            final Tuple2<T, NeighborList> tuple) {

        int[] degrees = new int[partitions];
        for (Neighbor<T> neighbor : tuple._2) {
            degrees[getColor(neighbor.getNode())]++;
        }

        return new Tuple2<Integer, int[]>(
                Integer.valueOf(tuple._1.id), degrees);

    }

    private int getColor(final Node<T> node) {
        return color_index.get(node.id);
    }

}

/**
 * Return the Tuple node.id => partition.
 * @author Thibault Debatty
 * @param <T>
 */
class GetPartitionFunction<T>
    implements PairFunction<Tuple2<Node<T>, NeighborList>, Long, Integer> {

    @Override
    public Tuple2<Long, Integer> call(
            final Tuple2<Node<T>, NeighborList> tuple)
            throws Exception {

        long id = tuple._1.id;
        int partition = tuple._1.partition;

        return new Tuple2<>(id, partition);
    }

}

class SwapRequest<T> implements Serializable {
    public final T src;
    public final T dst;
    public final int src_color;
    public final int dst_color;

    SwapRequest(
            final T src,
            final T dst,
            final int src_color,
            final int dst_color) {

        this.src = src;
        this.src_color = src_color;
        this.dst = dst;
        this.dst_color = dst_color;
    }

    @Override
    public String toString() {
        return src.id + " <> " + dst.id;
    }
}

class SwapResult<T> {

    public final JavaPairRDD<T, NeighborList> graph;
    public final int swaps;

    SwapResult(
            final JavaPairRDD<T, NeighborList> graph, final int swaps) {
        this.graph = graph;
        this.swaps = swaps;
    }

}