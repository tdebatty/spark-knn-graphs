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

package info.debatty.spark.knngraphs;

import info.debatty.java.datasets.gaussian.Dataset;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.StatisticsContainer;
import info.debatty.spark.knngraphs.builder.Brute;
import info.debatty.spark.knngraphs.builder.StatisticsAccumulator;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import junit.framework.TestCase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author Thibault Debatty
 */
public class ApproximateSearchTest extends TestCase implements Serializable {

    private static final int N = 10000;
    private static final int N_TEST = 100;
    private static final int N_CORRECT = 60;
    private static final int K = 10;
    private static final int DIMENSIONALITY = 1;
    private static final int NUM_CENTERS = 3;
    private static final int PARTITIONS = 8;
    private static final int ITERATIONS = 5;
    private static final double SPEEDUP = 4;

    /**
     * Test of search method, of class ApproximateSearch.
     * @throws java.lang.Exception if we cannot build the graph
     */
    public final void testSearch() throws Exception {
        System.out.println("Search");
        System.out.println("======");

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

        ExhaustiveSearch<double[]> exhaustive_search =
                new ExhaustiveSearch<double[]>(graph, new L2Similarity());

        System.out.println("Prepare the graph for approximate search");
        ApproximateSearch<double[]> approximate_search =
                new ApproximateSearch<double[]>(
                        graph,
                        new L2Similarity());


        System.out.println("Perform some search queries...");
        int correct = 0;
        for (int i = 0; i < N_TEST; i++) {
            double[] point = dataset.next();
            Node<double[]> query =
                    new Node<double[]>(
                            String.valueOf(data.size()),
                            point);

            StatisticsAccumulator stats_accumulator =
                    new StatisticsAccumulator();

            sc.sc().register(stats_accumulator);

            NeighborList approximate_result = approximate_search.search(
                    query,
                    1,
                    stats_accumulator);

            System.out.println(stats_accumulator);
            NeighborList exhaustive_result = exhaustive_search.search(query, 1);
            correct += approximate_result.countCommons(exhaustive_result);
        }
        System.out.println("Found " + correct + " correct responses");
        sc.close();
        // assertTrue(correct > N_CORRECT);
    }

    /**
     * Perform a search test, with zero partitioning iterations (hence
     * graph nodes are randomly distributed over compute nodes).
     * @throws Exception if we cannot build the graph
     */
    public final void testSearchWithouPartitioning() throws Exception {
        System.out.println("Search with random partitioning");
        System.out.println("===============================");

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

        ExhaustiveSearch<double[]> exhaustive_search =
                new ExhaustiveSearch<double[]>(graph, new L2Similarity());

        System.out.println("Prepare the graph for approximate search");
        ApproximateSearch<double[]> approximate_search =
                new ApproximateSearch<double[]>(
                        graph,
                        new L2Similarity());


        System.out.println("Perform some search queries...");
        int correct = 0;
        for (int i = 0; i < N_TEST; i++) {
            double[] point = dataset.next();
            Node<double[]> query =
                    new Node<double[]>(
                            String.valueOf(data.size()),
                            point);

            StatisticsAccumulator stats_accumulator =
                    new StatisticsAccumulator();

            sc.sc().register(stats_accumulator);

            NeighborList approximate_result = approximate_search.search(
                    query,
                    1,
                    stats_accumulator);

            System.out.println(stats_accumulator);
            NeighborList exhaustive_result = exhaustive_search.search(query, 1);
            correct += approximate_result.countCommons(exhaustive_result);
        }
        System.out.println("Found " + correct + " correct responses");
        sc.close();
    }
}
