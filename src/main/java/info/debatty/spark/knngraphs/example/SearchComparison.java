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
        
        if (args.length != 5) {
            System.out.println("Usage: ... <dataset> <partitioning_iterations> <partitioning_medoids> <gnss_restarts> <gnss_depth>");
            System.exit(1);
        }
        
        String file = args[0];

        // Graph building parameters
        int k = 10;

        // Partitioning parameters
        int partitioning_iterations = Integer.valueOf(args[1]);
        int partitioning_medoids = Integer.valueOf(args[2]);

        // Search parameters
        final int search_k = 10;
        final double search_expansion = 1.01;
        final int gnss_restarts = Integer.valueOf(args[3]);
        final int gnss_depth = Integer.valueOf(args[4]);

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
        
        java.util.Date date = new java.util.Date();
        System.out.println(date);
        System.out.println("Dataset:                 " + file);
        System.out.println("k:                       " + k);
        System.out.println("Partitioning iterations: " + partitioning_iterations);
        System.out.println("Partitioning medoids:    " + partitioning_medoids);
        System.out.println("GNSS restarts:           " + gnss_restarts);
        System.out.println("GNSS depth:              " + gnss_depth);
        System.out.println("GNSS expansion:          " + search_expansion);

        // Read the dataset file
        ArrayList<String> strings = DistributedGraphBuilder.readFile(file);

        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setIfMissing("spark.master", "local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        for (int validation_i = 0; validation_i < validation_iterations; validation_i++) {
            
            int correct_results = 0;
            int computed_similarities_graph = 0;
            long time_search_graph = 0;
            long time_search_exhaustive = 0;
            long time_build_graph = 0;
            long time_partition_graph = 0;
            long start_time = 0;
            
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

            // Parallelize the dataset and force execution
            JavaRDD<Node<String>> nodes = sc.parallelize(dataset);
            nodes.cache();
            nodes.first();

            // Compute the graph (and force execution)...
            start_time = System.currentTimeMillis();
            DistributedGraphBuilder<String> builder = new Brute<String>();
            builder.setK(k);
            builder.setSimilarity(similarity);
            JavaPairRDD<Node<String>, NeighborList> graph = builder.computeGraph(nodes);
            graph.cache();
            graph.first();
            time_build_graph = System.currentTimeMillis() - start_time;

            // Prepare the graph for searching (and force execution)
            start_time = System.currentTimeMillis();
            ApproximateSearch approximate_search_algorithm = new ApproximateSearch(graph, partitioning_iterations, partitioning_medoids, similarity);
            ExhaustiveSearch exhaustive_search = new ExhaustiveSearch(graph, similarity);
            graph.cache();
            graph.first();
            time_partition_graph = System.currentTimeMillis() - start_time;

            // Perform validation...
            for (final Node<String> query : validation_dataset) {

                // Using distributed graph based NN-search
                start_time = System.currentTimeMillis();
                int[] computed_similarities_temp = new int[1];
                NeighborList neighborlist_graph = 
                        approximate_search_algorithm.search(
                                query, 
                                search_k, 
                                gnss_restarts, 
                                gnss_depth, 
                                search_expansion,
                                computed_similarities_temp);
                time_search_graph += (System.currentTimeMillis() - start_time);
                computed_similarities_graph += computed_similarities_temp[0];

                // Using distributed exhaustive search
                start_time = System.currentTimeMillis();
                NeighborList neighborlist_exhaustive = exhaustive_search.search(query, search_k);
                time_search_exhaustive += (System.currentTimeMillis() - start_time);

                correct_results += neighborlist_graph.CountCommonValues(neighborlist_exhaustive);

            }

            int computed_similarities_exhaustive_search = (strings.size() - validation_queries) * validation_queries;

            // Correct search results
            System.out.printf("%d; %.1f; %d; %d; %f; %f; %d; %d; %d; %d\n",
                    correct_results,
                    100.0 * correct_results / (search_k * validation_queries),
                    computed_similarities_graph,
                    computed_similarities_exhaustive_search,
                    (double) computed_similarities_exhaustive_search / computed_similarities_graph,
                    (double) correct_results / (validation_queries * search_k) * computed_similarities_exhaustive_search / computed_similarities_graph,
                    time_search_graph,
                    time_search_exhaustive,
                    time_build_graph,
                    time_partition_graph);
        }
        
        System.out.println("SUCCESS!");
    }
}
