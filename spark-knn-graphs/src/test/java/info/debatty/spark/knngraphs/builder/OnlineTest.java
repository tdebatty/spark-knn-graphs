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
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.java.graphs.StatisticsContainer;
import info.debatty.spark.knngraphs.L2Similarity;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import junit.framework.TestCase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author Thibault Debatty
 */
public class OnlineTest extends TestCase implements Serializable {

    // Number of nodes in the initial graph
    private static final int N = 1000;

    // Number of nodes to add to the graph
    private static final int N_TEST = 200;
    private static final int PARTITIONS = 4;
    private static final int K = 10;
    private static final double SUCCESS_RATIO = 0.7;
    private static final int DIMENSIONALITY = 1;
    private static final int NUM_CENTERS = 4;
    private static final double SPEEDUP = 4;

    /**
     * Test of addNode method, of class Online.
     *
     * @throws Exception if the initial graph cannot be computed
     */
    public final void testAddNode() throws Exception {
        System.out.println("Add nodes");
        System.out.println("=========");

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        System.out.println("Create some random nodes");

        List<Node<double[]>> data = new ArrayList<Node<double[]>>();
        Iterator<double[]> dataset
                = new Dataset.Builder(DIMENSIONALITY, NUM_CENTERS)
                .setOverlap(Dataset.Builder.Overlap.MEDIUM)
                .build()
                .iterator();

        while (data.size() < N) {
            double[] point = dataset.next();
            data.add(new Node<double[]>(
                    String.valueOf(data.size()),
                    point));
        }

        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setIfMissing("spark.master", "local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Parallelize the dataset in Spark
        JavaRDD<Node<double[]>> nodes = sc.parallelize(data);

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
                new Online<double[]>(
                        K,
                        new L2Similarity(),
                        sc,
                        graph,
                        PARTITIONS);

        System.out.println("Add " + N_TEST + "nodes...");
        long start_time = System.currentTimeMillis();
        for (int i = 0; i < N_TEST; i++) {
            double[] point = dataset.next();
            Node<double[]> new_node =
                    new Node<double[]>(
                            String.valueOf(data.size()),
                            point);

            StatisticsAccumulator stats_accumulator
                    = new StatisticsAccumulator();
            sc.sc().register(stats_accumulator);

            online_graph.fastAdd(new_node, stats_accumulator);
            //System.out.println(stats_accumulator.value());

            // keep the node for later testing
            data.add(new_node);
        }
        System.out.println("Time: "
                + (System.currentTimeMillis() - start_time)
                + " ms");

        Graph<double[]> local_approximate_graph =
                list2graph(online_graph.getGraph().collect());

        System.out.println("Approximate graph size: "
                + online_graph.getGraph().collect().size());

        System.out.println("Compute the exact graph...");
        Graph<Double> local_exact_graph =
                list2graph(brute.computeGraph(sc.parallelize(data)).collect());

        sc.close();

        int correct = 0;
        for (Node<Double> node : local_exact_graph.getNodes()) {
            correct += local_exact_graph.get(node).countCommons(
                    local_approximate_graph.get(node));
        }

        double ratio = 1.0 * correct / (data.size() * K);
        System.out.printf("Found %d correct edges (%f)\n", correct, ratio);
        System.out.println("Number of partitions: "
                + online_graph.getGraph().partitions().size());

        assertEquals(online_graph.getGraph().partitions().size(), PARTITIONS);
        assertEquals(data.size(), local_approximate_graph.size());
        assertTrue(ratio > SUCCESS_RATIO);
    }

    /**
     * Test fastRemove().
     * @throws Exception if we cannot build the graph.
     */
    public final void testRemove() throws Exception {

        System.out.println("Remove nodes");
        System.out.println("============");

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        System.out.println("Create some random nodes");
        List<Node<double[]>> data = new ArrayList<Node<double[]>>();
        Iterator<double[]> dataset
                = new Dataset.Builder(DIMENSIONALITY, NUM_CENTERS)
                .setOverlap(Dataset.Builder.Overlap.MEDIUM)
                .build()
                .iterator();

        while (data.size() < N) {
            double[] point = dataset.next();
            data.add(new Node<double[]>(
                    String.valueOf(data.size()),
                    point));
        }

        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setIfMissing("spark.master", "local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Parallelize the dataset in Spark
        JavaRDD<Node<double[]>> nodes = sc.parallelize(data);

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
                new Online<double[]>(K, new L2Similarity(), sc, graph, PARTITIONS);

        System.out.println("Remove some nodes...");
        LinkedList<Node<Double>> removed_nodes = new LinkedList<Node<Double>>();

        Random rand = new Random();
        for (int i = 0; i < N_TEST; i++) {
            Node query = data.get(rand.nextInt(data.size() - 1));

            StatisticsAccumulator stats_accumulator
                    = new StatisticsAccumulator();
            sc.sc().register(stats_accumulator);

            online_graph.fastRemove(query, stats_accumulator);
            if (i == 0) {
                System.out.println(stats_accumulator.value());
            }
            data.remove(query);
            removed_nodes.add(query);
        }
        Graph<double[]> local_approximate_graph =
                list2graph(online_graph.getGraph().collect());

        System.out.println("Compute the exact graph...");
        Graph<Double> local_exact_graph =
                list2graph(brute.computeGraph(sc.parallelize(data)).collect());

        sc.close();

        int correct = 0;
        for (Node<Double> node : local_exact_graph.getNodes()) {
            try {
            correct += local_exact_graph.get(node).countCommons(
                    local_approximate_graph.get(node));
            } catch (Exception ex) {
                System.out.println("Null neighborlist!");
            }
        }

        // Check all nodes have K neighbors
        for (Node<double[]> node : local_approximate_graph.getNodes()) {
            assertEquals(K, local_approximate_graph.get(node).size());

            // Check the old nodes are completely removed
            assertTrue(!node.equals(removed_nodes.get(0)));
            for (Neighbor neighbor : local_approximate_graph.get(node)) {
                assertTrue(!neighbor.node.equals(removed_nodes.get(0)));
            }
        }

        double ratio = 1.0 * correct / (data.size() * K);
        System.out.printf("Found %d correct edges (%f)\n", correct, ratio);

        assertEquals(data.size(), local_approximate_graph.size());
        assertEquals(online_graph.getGraph().partitions().size(), PARTITIONS);
        assertTrue(ratio > SUCCESS_RATIO);
    }

    private Graph<double[]> list2graph(
            final List<Tuple2<Node<double[]>, NeighborList>> list) {

        Graph<double[]> graph = new Graph<double[]>();
        for (Tuple2<Node<double[]>, NeighborList> tuple : list) {
            graph.put(tuple._1, tuple._2);
        }

        return graph;
    }
}
