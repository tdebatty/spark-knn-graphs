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

        SimilarityInterface<Double> similarity =
                new SimilarityInterface<Double>() {

            public double similarity(final Double value1, final Double value2) {
                return 1.0 / (1 + Math.abs(value1 - value2));
            }
        };

        System.out.println("Create some random nodes");

        List<Node<Double>> data = new ArrayList<Node<Double>>();
        Iterator<Double[]> dataset
                = new Dataset.Builder(DIMENSIONALITY, NUM_CENTERS)
                .setOverlap(Dataset.Builder.Overlap.MEDIUM)
                .build()
                .iterator();

        while (data.size() < N) {
            Double[] point = dataset.next();
            data.add(new Node<Double>(
                    String.valueOf(data.size()),
                    point[0]));
        }

        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setIfMissing("spark.master", "local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Parallelize the dataset in Spark
        JavaRDD<Node<Double>> nodes = sc.parallelize(data);

        Brute brute = new Brute();
        brute.setK(K);
        brute.setSimilarity(similarity);

        System.out.println("Compute the graph and force execution");
        JavaPairRDD<Node<Double>, NeighborList> graph
                = brute.computeGraph(nodes);
        graph.cache();
        graph.count();

        System.out.println("Prepare the graph for online processing");
        Online<Double> online_graph =
                new Online<Double>(K, similarity, sc, graph, PARTITIONS);
        online_graph.setSearchSpeedup(SPEEDUP);

        System.out.println("Add some nodes...");
        long start_time = System.currentTimeMillis();
        for (int i = 0; i < N_TEST; i++) {
            Double[] point = dataset.next();
            Node<Double> new_node =
                    new Node<Double>(
                            String.valueOf(data.size()),
                            point[0]);

            Accumulator<StatisticsContainer> stats_accumulator
                    = sc.accumulator(
                            new StatisticsContainer(),
                            new StatisticsAccumulator());

            online_graph.fastAdd(new_node, stats_accumulator);
            //System.out.println(stats_accumulator.value());

            // keep the node for later testing
            data.add(new_node);
        }
        System.out.println("Time: "
                + (System.currentTimeMillis() - start_time)
                + " ms");

        Graph<Double> local_approximate_graph =
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
     * Test the fastAdd algorithm without partitioning (zero partitioning
     * iterations).
     * @throws Exception if we cannot build the graph.
     */
    public final void testWithoutPartitioning() throws Exception {
        if (true) {
            return;
        }

        System.out.println("Add nodes without partitioning");
        System.out.println("==============================");

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        SimilarityInterface<Double> similarity =
                new SimilarityInterface<Double>() {

            public double similarity(final Double value1, final Double value2) {
                return 1.0 / (1 + Math.abs(value1 - value2));
            }
        };

        System.out.println("Create some random nodes");

        List<Node<Double>> data = new ArrayList<Node<Double>>();
        Iterator<Double[]> dataset
                = new Dataset.Builder(DIMENSIONALITY, NUM_CENTERS)
                .setOverlap(Dataset.Builder.Overlap.MEDIUM)
                .build()
                .iterator();

        while (data.size() < N) {
            Double[] point = dataset.next();
            data.add(new Node<Double>(
                    String.valueOf(data.size()),
                    point[0]));
        }

        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setIfMissing("spark.master", "local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Parallelize the dataset in Spark
        JavaRDD<Node<Double>> nodes = sc.parallelize(data);

        Brute brute = new Brute();
        brute.setK(K);
        brute.setSimilarity(similarity);

        System.out.println("Compute the graph and force execution");
        JavaPairRDD<Node<Double>, NeighborList> graph
                = brute.computeGraph(nodes);
        graph.cache();
        graph.count();

        System.out.println("Prepare the graph for online processing");
        Online<Double> online_graph =
                new Online<Double>(K, similarity, sc, graph, PARTITIONS, 0);
        online_graph.setSearchSpeedup(SPEEDUP);

        System.out.println("Add some nodes...");
        for (int i = 0; i < N_TEST; i++) {
            Double[] point = dataset.next();
            Node<Double> new_node =
                    new Node<Double>(
                            String.valueOf(data.size()),
                            point[0]);
            online_graph.fastAdd(new_node);

            // keep the node for later testing
            data.add(new_node);
        }
        Graph<Double> local_approximate_graph =
                list2graph(online_graph.getGraph().collect());

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

        assertEquals(data.size(), local_approximate_graph.size());
        assertEquals(online_graph.getGraph().partitions().size(), PARTITIONS);
    }

    /**
     * Test fastRemove().
     * @throws Exception if we cannot build the graph.
     */
    public final void testRemove() throws Exception {
        if (true) {
            return;
        }

        System.out.println("Remove nodes");
        System.out.println("============");

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        SimilarityInterface<Double> similarity =
                new SimilarityInterface<Double>() {

            public double similarity(final Double value1, final Double value2) {
                return 1.0 / (1 + Math.abs(value1 - value2));
            }
        };

        System.out.println("Create some random nodes");
        List<Node<Double>> data = new ArrayList<Node<Double>>();
        Iterator<Double[]> dataset
                = new Dataset.Builder(DIMENSIONALITY, NUM_CENTERS)
                .setOverlap(Dataset.Builder.Overlap.MEDIUM)
                .build()
                .iterator();

        while (data.size() < N) {
            Double[] point = dataset.next();
            data.add(new Node<Double>(
                    String.valueOf(data.size()),
                    point[0]));
        }

        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setIfMissing("spark.master", "local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Parallelize the dataset in Spark
        JavaRDD<Node<Double>> nodes = sc.parallelize(data);

        Brute brute = new Brute();
        brute.setK(K);
        brute.setSimilarity(similarity);

        System.out.println("Compute the graph and force execution");
        JavaPairRDD<Node<Double>, NeighborList> graph
                = brute.computeGraph(nodes);
        graph.cache();
        graph.count();

        System.out.println("Prepare the graph for online processing");
        Online<Double> online_graph =
                new Online<Double>(K, similarity, sc, graph, PARTITIONS);
        online_graph.setSearchSpeedup(SPEEDUP);

        System.out.println("Remove some nodes...");
        LinkedList<Node<Double>> removed_nodes = new LinkedList<Node<Double>>();

        Random rand = new Random();
        for (int i = 0; i < N_TEST; i++) {
            Node query = data.get(rand.nextInt(data.size() - 1));
            online_graph.fastRemove(query);
            data.remove(query);
            removed_nodes.add(query);
        }
        Graph<Double> local_approximate_graph =
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
        for (Node<Double> node : local_approximate_graph.getNodes()) {
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

    /**
     * Test add + remove (sliding window).
     * @throws Exception if we cannot build the graph.
     */
    public final void testWindow() throws Exception {
        if (true) {
            return;
        }

        System.out.println("Sliding window");
        System.out.println("==============");

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        SimilarityInterface<Double> similarity =
                new SimilarityInterface<Double>() {

            public double similarity(final Double value1, final Double value2) {
                return 1.0 / (1 + Math.abs(value1 - value2));
            }
        };

        System.out.println("Create some random nodes");
        List<Node<Double>> data = new ArrayList<Node<Double>>();
        Iterator<Double[]> dataset
                = new Dataset.Builder(DIMENSIONALITY, NUM_CENTERS)
                .setOverlap(Dataset.Builder.Overlap.MEDIUM)
                .build()
                .iterator();

        int sequence_number = 0;
        while (sequence_number < N) {
            Double[] point = dataset.next();

            Node<Double> node = new Node<Double>(
                    String.valueOf(sequence_number),
                    point[0]);
            node.setAttribute(Online.NODE_SEQUENCE_KEY, sequence_number);
            data.add(node);
            sequence_number++;
        }

        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setIfMissing("spark.master", "local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Parallelize the dataset in Spark
        JavaRDD<Node<Double>> nodes = sc.parallelize(data);

        Brute brute = new Brute();
        brute.setK(K);
        brute.setSimilarity(similarity);

        System.out.println("Compute the graph and force execution");
        JavaPairRDD<Node<Double>, NeighborList> graph
                = brute.computeGraph(nodes);
        graph.cache();
        graph.count();

        System.out.println("Prepare the graph for online processing");
        Online<Double> online_graph =
                new Online<Double>(K, similarity, sc, graph, PARTITIONS);
        online_graph.setSearchSpeedup(SPEEDUP);
        online_graph.setWindowSize(N);

        System.out.println("Add some nodes...");
        for (int i = 0; i < N_TEST; i++) {
            Double[] point = dataset.next();
            Node<Double> new_node =
                    new Node<Double>(
                            String.valueOf(sequence_number),
                            point[0]);
            new_node.setAttribute(Online.NODE_SEQUENCE_KEY, sequence_number);

            online_graph.fastAdd(new_node);
            sequence_number++;
        }
        Graph<Double> local_approximate_graph =
                list2graph(online_graph.getGraph().collect());
        sc.close();

        assertEquals(N, local_approximate_graph.size());
        assertEquals(online_graph.getGraph().partitions().size(), PARTITIONS);
    }

    private Graph<Double> list2graph(
            final List<Tuple2<Node<Double>, NeighborList>> list) {

        Graph<Double> graph = new Graph<Double>();
        for (Tuple2<Node<Double>, NeighborList> tuple : list) {
            graph.put(tuple._1, tuple._2);
        }

        return graph;
    }
}
