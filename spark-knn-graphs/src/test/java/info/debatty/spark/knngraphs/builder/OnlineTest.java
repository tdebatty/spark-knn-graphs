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

import info.debatty.java.datasets.gaussian.Dataset;
import info.debatty.java.graphs.Graph;
import info.debatty.java.graphs.Neighbor;
import info.debatty.java.graphs.NeighborList;
import info.debatty.spark.knngraphs.DistributedGraph;
import info.debatty.spark.knngraphs.KNNGraphCase;

import info.debatty.spark.knngraphs.L2Similarity;
import info.debatty.spark.knngraphs.Node;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author Thibault Debatty
 */
public class OnlineTest extends KNNGraphCase {

    // Number of nodes in the initial graph
    private static final int N = 2000;

    // Number of nodes to add to the graph
    private static final int N_TEST = 200;
    private static final int PARTITIONS = 4;
    private static final int K = 10;
    private static final double SUCCESS_RATIO = 0.95;
    private static final int DIMENSIONALITY = 1;
    private static final int NUM_CENTERS = 4;

    /**
     * Test of addNode method, of class Online.
     *
     * @throws Exception if the initial graph cannot be computed
     */
    public final void testAddNode() throws Exception {
        System.out.println("Add nodes");
        System.out.println("=========");


        System.out.println("Create some random nodes");

        Dataset dataset = new Dataset.Builder(DIMENSIONALITY, NUM_CENTERS)
                .setOverlap(Dataset.Builder.Overlap.MEDIUM)
                .setSize(N)
                .build();

        JavaSparkContext sc = getSpark();

        // Parallelize the dataset in Spark
        JavaRDD<double[]> nodes = sc.parallelize(dataset.get(N));

        Brute brute = new Brute();
        brute.setK(K);
        brute.setSimilarity(new L2Similarity());

        System.out.println("Compute the graph and force execution");
        JavaPairRDD<Node<double[]>, NeighborList> graph
                = brute.computeGraph(nodes);
        graph.cache();
        graph.count();

        System.out.println("Prepare the graph for online processing");
        Online<double[]> online_graph =
                new Online<>(
                        K,
                        new L2Similarity(),
                        sc,
                        graph,
                        PARTITIONS);

        System.out.println("Add " + N_TEST + " nodes...");
        long start_time = System.currentTimeMillis();
        for (int i = 0; i < N_TEST; i++) {
            double[] point = dataset.get(1).element();

            StatisticsAccumulator stats_accumulator
                    = new StatisticsAccumulator();
            sc.sc().register(stats_accumulator);

            online_graph.fastAdd(point, stats_accumulator);
            //System.out.println(stats_accumulator.value());

        }

        System.out.println("Time: "
                + (System.currentTimeMillis() - start_time)
                + " ms");


        JavaPairRDD<Node<double[]>, NeighborList> approximate_graph =
                online_graph.getGraph();

        System.out.println("Compute the exact graph...");
        JavaPairRDD exact_graph =
                brute.computeGraphFromNodes(online_graph.getGraph().keys());

        long correct_edges = DistributedGraph.countCommonEdges(
                exact_graph, approximate_graph);
        double correct_ratio = correct_edges / (1.0 * K * (N + N_TEST));

        System.out.printf("Found %d correct edges (%f)\n",
                correct_edges, correct_ratio);

        assertEquals(online_graph.getGraph().partitions().size(), PARTITIONS);
        assertEquals(N + N_TEST, approximate_graph.count());
        assertTrue(correct_ratio > SUCCESS_RATIO);
    }

    /**
     * Test fastRemove().
     * @throws Exception if we cannot build the graph.
     */
    public final void testRemove() throws Exception {

        System.out.println("Remove nodes");
        System.out.println("============");

        System.out.println("Create some random nodes");
        Dataset dataset = new Dataset.Builder(DIMENSIONALITY, NUM_CENTERS)
                .setOverlap(Dataset.Builder.Overlap.MEDIUM)
                .build();

        // Parallelize the dataset in Spark
        JavaSparkContext sc = getSpark();
        JavaRDD<double[]> nodes = sc.parallelize(dataset.get(N));

        System.out.println("Compute the graph and force execution");
        Brute brute = new Brute();
        brute.setK(K);
        brute.setSimilarity(new L2Similarity());
        JavaPairRDD<Node<double[]>, NeighborList> graph
                = brute.computeGraph(nodes);
        graph.cache();
        graph.count();

        System.out.println("Prepare the graph for online processing");
        Online<double[]> online_graph =
                new Online<>(K, new L2Similarity(), sc, graph, PARTITIONS);

        System.out.println("Remove some nodes...");
        Random rand = new Random();
        List<Node<double[]>> allnodes = new LinkedList<>(
                graph.keys().collect());
        List<Node<double[]>> removed_nodes = new LinkedList<>();

        for (int i = 0; i < N_TEST; i++) {
            Node<double[]> query =
                    allnodes.get(rand.nextInt(allnodes.size()));
            online_graph.fastRemove(query, null);
            allnodes.remove(query);
            removed_nodes.add(query);
        }

        JavaPairRDD<Node<double[]>, NeighborList> approximate_graph =
                online_graph.getGraph();

        assertEquals(N - N_TEST, approximate_graph.count());

        System.out.println("Compute the exact graph...");
        JavaPairRDD exact_graph = brute.computeGraphFromNodes(
                approximate_graph.keys());

        long correct_edges = DistributedGraph.countCommonEdges(
                exact_graph, approximate_graph);
        double correct_ratio = correct_edges / (1.0 * K * (N - N_TEST));
        System.out.printf("Found %d correct edges (%f)\n",
                correct_edges, correct_ratio);
        assertTrue(correct_ratio > SUCCESS_RATIO);

        Graph<Node<double[]>> local_approx_graph =
                list2graph(approximate_graph.collect());

        for (Node<double[]> node : local_approx_graph.getNodes()) {
            // Check all nodes have K neighbors
            assertEquals(K, local_approx_graph.getNeighbors(node).size());

            // Check the old nodes are completely removed
            assertTrue(!node.equals(removed_nodes.get(0)));
            for (Neighbor neighbor : local_approx_graph.getNeighbors(node)) {
                assertTrue(!neighbor.getNode().equals(removed_nodes.get(0)));
            }
        }

        assertEquals(approximate_graph.partitions().size(), PARTITIONS);
    }

    private Graph<Node<double[]>> list2graph(
            final List<Tuple2<Node<double[]>, NeighborList>> list) {

        Graph<Node<double[]>> graph = new Graph<>();
        for (Tuple2<Node<double[]>, NeighborList> tuple : list) {
            graph.put(tuple._1, tuple._2);
        }
        return graph;
    }
}
