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

import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.spark.knngraphs.builder.Brute;
import info.debatty.spark.knngraphs.builder.DistributedGraphBuilder;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author Thibault Debatty
 */
public class JaBeJaTest extends TestCase implements Serializable {

    private static final int K = 10;

    /**
     * Test of computeGraph method, of class NNDescent.
     * @throws java.io.IOException
     */
    public final void testBuildIndex() throws IOException, Exception {
        System.out.println("BuildIndex");
        System.out.println("==========");

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        String file =  getClass().getClassLoader().
                getResource("726-unique-spams").getPath();

        // Read the file
        ArrayList<String> strings = DistributedGraphBuilder.readFile(file);

        // Convert to nodes
        List<Node<String>> data = new ArrayList<Node<String>>();
        for (String s : strings) {
            data.add(new Node<String>(String.valueOf(data.size()), s));
        }

        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setIfMissing("spark.master", "local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Parallelize the dataset in Spark
        JavaRDD<Node<String>> nodes = sc.parallelize(data);

        // Build the graph
        Brute<String> builder = new Brute<String>();
        builder.setK(K);
        builder.setSimilarity(new JWSimilarity());

        // Compute the graph and force execution
        JavaPairRDD<Node<String>, NeighborList> graph =
                builder.computeGraph(nodes);
        Tuple2<Node<String>, NeighborList> first = graph.first();
        System.out.println(first);
        assertEquals(726, graph.count());
        assertEquals(K, first._2.size());

        JaBeJa<String> jbj = new JaBeJa<String>(8);
        graph = jbj.randomize(graph);
        graph.cache();
        first = graph.first();
        int first_partition = (Integer) first._1
                .getAttribute(NodePartitioner.PARTITION_KEY);
        int first_id = Integer.valueOf(first._1.id);

        int[] index = JaBeJa.buildColorIndex(graph);
        assertEquals(first_partition, index[first_id]);
        sc.close();
    }

    public final void testSwap() throws IOException, Exception {
        System.out.println("Swap");
        System.out.println("====");

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        Logger.getLogger("info").setLevel(Level.WARN);
        Logger.getLogger("info.debatty.spark.knngraphs.JaBeJa")
                .setLevel(Level.INFO);

        String file =  getClass().getClassLoader().
                getResource("726-unique-spams").getPath();

        // Read the file
        ArrayList<String> strings = DistributedGraphBuilder.readFile(file);

        // Convert to nodes
        List<Node<String>> data = new ArrayList<Node<String>>();
        for (String s : strings) {
            data.add(new Node<String>(String.valueOf(data.size()), s));
        }

        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setIfMissing("spark.master", "local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Parallelize the dataset in Spark
        JavaRDD<Node<String>> nodes = sc.parallelize(data);

        // Build the graph
        Brute<String> builder = new Brute<String>();
        builder.setK(K);
        builder.setSimilarity(new JWSimilarity());

        // Compute the graph and force execution
        JavaPairRDD<Node<String>, NeighborList> graph =
                builder.computeGraph(nodes);
        graph.cache();
        graph.count();

        JaBeJa<String> jbj = new JaBeJa<String>(8);

        // Randomize
        graph = jbj.randomize(graph);
        graph = DistributedGraph.moveNodes(graph, 8);
        graph.cache();

        testPartitionNotNull(graph);

        int cross_edges_before = JaBeJa.countCrossEdges(graph, 8);

        // Perform Swap
        graph = jbj.swap(graph, 2.0, 1).graph;
        graph = DistributedGraph.moveNodes(graph, 8);
        graph.cache();
        graph.count();
        testPartitionNotNull(graph);

        int cross_edges_after = JaBeJa.countCrossEdges(graph, 8);

        assertTrue("Number of cross edges should decrease!",
                cross_edges_after < cross_edges_before);

        sc.close();
    }

    public final void testTimeBudget() throws IOException, Exception {
        System.out.println("TimeBudget");
        System.out.println("==========");

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        //Logger.getLogger("info").setLevel(Level.WARN);
        Logger.getLogger("info.debatty.spark.knngraphs.JaBeJa")
                .setLevel(Level.INFO);

        String file =  getClass().getClassLoader().
                getResource("726-unique-spams").getPath();

        // Read the file
        ArrayList<String> strings = DistributedGraphBuilder.readFile(file);

        // Convert to nodes
        List<Node<String>> data = new ArrayList<Node<String>>();
        for (String s : strings) {
            data.add(new Node<String>(String.valueOf(data.size()), s));
        }

        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setIfMissing("spark.master", "local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Parallelize the dataset in Spark
        JavaRDD<Node<String>> nodes = sc.parallelize(data);

        // Build the graph
        Brute<String> builder = new Brute<String>();
        builder.setK(K);
        builder.setSimilarity(new JWSimilarity());

        // Compute the graph and force execution
        JavaPairRDD<Node<String>, NeighborList> graph =
                builder.computeGraph(nodes);
        graph.cache();
        graph.count();

        JaBeJa<String> jbj = new JaBeJa<String>(8);
        jbj.setBudget(new TimeBudget(10)); // 10 seconds
        Partitioning<String> solution = jbj.partition(graph);
        System.out.println(solution.runTime());
        System.out.println(JaBeJa.countCrossEdges(solution.graph, 8));
        testPartitionNotNull(solution.graph);

        sc.close();
    }

    public final void testPartition() throws IOException, Exception {
        System.out.println("Partition");
        System.out.println("=========");

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        Logger.getLogger("info").setLevel(Level.WARN);
        Logger.getLogger("info.debatty.spark.knngraphs.JaBeJa")
                .setLevel(Level.INFO);

        String file =  getClass().getClassLoader().
                getResource("726-unique-spams").getPath();

        // Read the file
        ArrayList<String> strings = DistributedGraphBuilder.readFile(file);

        // Convert to nodes
        List<Node<String>> data = new ArrayList<Node<String>>();
        for (String s : strings) {
            data.add(new Node<String>(String.valueOf(data.size()), s));
        }

        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setIfMissing("spark.master", "local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Parallelize the dataset in Spark
        JavaRDD<Node<String>> nodes = sc.parallelize(data);

        // Build the graph
        Brute<String> builder = new Brute<String>();
        builder.setK(K);
        builder.setSimilarity(new JWSimilarity());

        // Compute the graph and force execution
        JavaPairRDD<Node<String>, NeighborList> graph =
                builder.computeGraph(nodes);
        graph.repartition(8);
        graph.cache();
        graph.count();

        JaBeJa<String> jbj = new JaBeJa<String>(8);
        graph = jbj.partition(graph).graph;
        System.out.println(JaBeJa.countCrossEdges(graph, 8));
        testPartitionNotNull(graph);

        sc.close();
    }

    /**
     * Test that every node has a non null partition value.
     * @param graph
     */
    private void testPartitionNotNull(
            final JavaPairRDD<Node<String>, NeighborList> graph) {

        for (Tuple2<Node<String>, NeighborList> tuple : graph.collect()) {
            assertNotNull("Partition id is null!", tuple._1.getAttribute(
                    NodePartitioner.PARTITION_KEY));
        }

    }

}
