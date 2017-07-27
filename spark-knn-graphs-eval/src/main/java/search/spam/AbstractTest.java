/*
 * The MIT License
 *
 * Copyright 2017 tibo.
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
package search.spam;

import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.jinu.TestInterface;
import info.debatty.spark.knngraphs.ApproximateSearch;
import info.debatty.spark.knngraphs.ExhaustiveSearch;
import info.debatty.spark.knngraphs.Partitioner;
import info.debatty.spark.knngraphs.Partitioning;
import info.debatty.spark.knngraphs.TimeBudget;
import info.debatty.spark.knngraphs.eval.JWSimilarity;
import java.util.LinkedList;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author tibo
 */
public abstract class AbstractTest implements TestInterface {

    static String graph_path;
    static LinkedList<String> queries;

    abstract Partitioner<String> getPartitioner();

    @Override
    public final double[] run(final double budget) throws Exception {

        SparkConf conf = new SparkConf();
        conf.setAppName("Spark graph partitioning with SPAM");
        conf.setIfMissing("spark.master", "local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Tuple2<Node<String>, NeighborList>> tuples =
                sc.objectFile(graph_path);
        JavaPairRDD<Node<String>, NeighborList> graph =
                JavaPairRDD.fromJavaRDD(tuples);

        Partitioner<String> partitioner = getPartitioner();
        partitioner.setBudget(new TimeBudget((long) budget));
        Partitioning<String> partition = partitioner.partition(graph);

        // Use default parameters
        ApproximateSearch<String> fast_search = new ApproximateSearch<>(
                partition.graph,
                new JWSimilarity());

        ExhaustiveSearch<String> search = new ExhaustiveSearch<>(
                partition.graph, new JWSimilarity());

        int correct = 0;
        for (String query : queries) {
            Node<String> query_node = new Node(query, query);
            NeighborList fast_result = fast_search.search(query_node, 1);
            NeighborList exact_result = search.search(query_node, 1);

            correct += fast_result.countCommons(exact_result);
        }

        double[] result = new double[] {
            correct
        };
        sc.close();

        return result;
    }
}
