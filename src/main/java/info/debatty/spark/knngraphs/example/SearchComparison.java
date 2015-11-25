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
public class SearchComparison {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException, Exception {
        String file = args[0];

        // Graph building parameters
        int k = 10;

        // Partitioning parameters
        int partitioning_iterations = 6;
        int partitioning_medoids = 4;

        // Search parameters
        final int search_k = 10;
        final int gnss_restarts = 5;
        final int gnss_depth = 5;

        // Cross-validation parameters
        // Repeated random sub-sampling validation
        int validation_iterations = 10;
        int validation_queries = 100;

        // Similarity measure
        final SimilarityInterface<String> similarity = new SimilarityInterface<String>() {

            public double similarity(String value1, String value2) {
                JaroWinkler jw = new JaroWinkler();
                return jw.similarity(value1, value2);
            }
        };

        // Read the dataset file
        ArrayList<String> strings = DistributedGraphBuilder.readFile(file);
        System.out.printf("Found %d strings in input file\n", strings.size());

        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setIfMissing("spark.master", "local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        for (int validation_i = 0; validation_i < validation_iterations; validation_i++) {
            // Convert to nodes
            List<Node<String>> dataset = new ArrayList<Node<String>>();
            for (String s : strings) {
                dataset.add(new Node<String>(String.valueOf(dataset.size()), s));
            }

            // Split the dataset between training and validation
            Random rand = new Random();
            ArrayList<Node<String>> validation_dataset = new ArrayList<Node<String>>(validation_queries);
            for (int i = 0; i < validation_queries; i++) {
                validation_dataset.add(dataset.remove(rand.nextInt(dataset.size())));
            }

            // Parallelize the dataset
            JavaRDD<Node<String>> nodes = sc.parallelize(dataset);

            // Compute the graph...
            System.out.println("Computing graph...");
            DistributedGraphBuilder<String> builder = new Brute<String>();
            builder.setK(k);
            builder.setSimilarity(similarity);
            JavaPairRDD<Node<String>, NeighborList> graph = builder.computeGraph(nodes);
            graph.cache();

            // Prepare the graph for searching
            ApproximateSearch approximate_search_algorithm = new ApproximateSearch(graph, partitioning_iterations, partitioning_medoids, similarity);
            ExhaustiveSearch exhaustive_search = new ExhaustiveSearch(graph, similarity);

            // Perform validation...
            int correct_results = 0;
            int computed_similarities_graph = 0;
            long running_time_graph = 0;
            long running_time_exhaustive_search = 0;

            for (final Node<String> query : validation_dataset) {

                System.out.println("Search query: " + query.value);

                // Using distributed graph based NN-search
                long start_time = System.currentTimeMillis();
                int[] computed_similarities_temp = new int[1];
                NeighborList neighborlist_graph = approximate_search_algorithm.search(query, search_k, gnss_restarts, gnss_depth, computed_similarities_temp);
                running_time_graph += (System.currentTimeMillis() - start_time);
                computed_similarities_graph += computed_similarities_temp[0];

                // Using distributed exhaustive search
                start_time = System.currentTimeMillis();
                NeighborList neighborlist_exhaustive = exhaustive_search.search(query, search_k);
                running_time_exhaustive_search += (System.currentTimeMillis() - start_time);

                correct_results += neighborlist_graph.CountCommons(neighborlist_exhaustive);

            }

            int computed_similarities_exhaustive_search = (strings.size() - validation_queries) * validation_queries;

            // Correct search results
            System.out.printf("%d; ", correct_results);

            // Precision (%)
            System.out.printf("%.1f; ", 100.0 * correct_results / (search_k * validation_queries));

            // Computed similarities (graph based)
            System.out.printf("%d; ", computed_similarities_graph);

            // Computed similarities (exhaustive search)
            System.out.printf("%d; ", computed_similarities_exhaustive_search);

            // Gross speedup
            System.out.printf("%f; ", (double) computed_similarities_exhaustive_search / computed_similarities_graph);

            // Quality-equivalent speedup
            System.out.printf("%f; ", (double) correct_results / (validation_queries * search_k) * computed_similarities_exhaustive_search / computed_similarities_graph);

            // Graph based running time
            System.out.printf("%d; ", running_time_graph);

            // Exhaustive search running time
            System.out.printf("%d; ", running_time_exhaustive_search);

            System.out.println();
        }
    }
}
