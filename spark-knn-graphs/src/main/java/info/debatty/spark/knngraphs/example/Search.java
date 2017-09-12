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
package info.debatty.spark.knngraphs.example;

import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.java.stringsimilarity.JaroWinkler;
import info.debatty.spark.knngraphs.ApproximateSearch;
import info.debatty.spark.knngraphs.ExhaustiveSearch;
import info.debatty.spark.knngraphs.builder.Brute;
import info.debatty.spark.knngraphs.builder.DistributedGraphBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author Thibault Debatty
 */
public final class Search {

    /**
     *
     */
    private Search() {
    }

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     * @throws Exception
     */
    public static void main(final String[] args) throws IOException, Exception {
        if (args.length != 1) {
            System.out.println(
                    "Usage: spark-submit --class "
                    + Search.class.getCanonicalName() + " "
                    + "<dataset>");
        }

        String file = args[0];

        // Graph building parameters
        int k = 10;

        // Partitioning parameters
        int partitioning_iterations = 5;
        int partitioning_medoids = 4;

        // Search parameters
        int search_k = 10;
        double speedup = 4.0;

        int search_queries = 10;

        // Similarity measure
        final SimilarityInterface<String> similarity
                = new SimilarityInterface<String>() {

                    public double similarity(
                            final String value1,
                            final String value2) {
                        JaroWinkler jw = new JaroWinkler();
                        return jw.similarity(value1, value2);
                    }
                };

        // Read the dataset file
        ArrayList<String> strings = DistributedGraphBuilder.readFile(file);

        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setIfMissing("spark.master", "local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Convert to nodes
        List<Node<String>> dataset = new ArrayList<Node<String>>();
        for (String s : strings) {
            dataset.add(new Node<String>(String.valueOf(dataset.size()), s));
        }

        // Split the dataset between training and validation
        Random rand = new Random();
        ArrayList<Node<String>> validation_dataset
                = new ArrayList<Node<String>>(search_queries);
        for (int i = 0; i < search_queries; i++) {
            validation_dataset.add(
                    dataset.remove(rand.nextInt(dataset.size())));
        }

        // Parallelize the dataset and force execution
        JavaRDD<Node<String>> nodes = sc.parallelize(dataset);
        nodes.cache();
        nodes.first();

        // Compute the graph (and force execution)...
        DistributedGraphBuilder<String> builder = new Brute<String>();
        builder.setK(k);
        builder.setSimilarity(similarity);
        JavaPairRDD<Node<String>, NeighborList> graph
                = builder.computeGraph(nodes);
        graph.cache();
        graph.first();

        // Prepare the graph for approximate graph based search
        // (and force execution)
        ApproximateSearch approximate_search_algorithm
                = new ApproximateSearch(graph, similarity, partitioning_medoids);

        // Prepare exhaustive search
        ExhaustiveSearch exhaustive_search
                = new ExhaustiveSearch(graph, similarity);
        graph.cache();
        graph.first();

        // Perform some search...
        for (final Node<String> query : validation_dataset) {
            System.out.println("Search query: " + query.value);

            // Using distributed graph based NN-search
            NeighborList neighborlist_graph
                    = approximate_search_algorithm.search(query, search_k);
            System.out.println(
                    "Using graph: " + neighborlist_graph.element().node.value);

            // Using distributed exhaustive search
            NeighborList neighborlist_exhaustive
                    = exhaustive_search.search(query, search_k);
            System.out.println(
                    "Using exhaustive search: "
                    + neighborlist_exhaustive.element().node.value);
        }
    }
}
