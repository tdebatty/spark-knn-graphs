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
package search.synthetic;

import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.jinu.TestInterface;
import info.debatty.spark.knngraphs.ApproximateSearch;
import info.debatty.spark.knngraphs.ExhaustiveSearch;
import info.debatty.spark.knngraphs.partitioner.Partitioner;
import info.debatty.spark.knngraphs.partitioner.Partitioning;
import info.debatty.spark.knngraphs.eval.L2Similarity;
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
    static LinkedList<double[]> queries;

    abstract Partitioner<double[]> getPartitioner(int budget);

    @Override
    public final double[] run(final double budget) throws Exception {

        SparkConf conf = new SparkConf();
        conf.setAppName("Spark graph partitioning with synthetic dataset");
        conf.setIfMissing("spark.master", "local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Tuple2<Node<double[]>, NeighborList>> tuples =
                sc.objectFile(graph_path);
        JavaPairRDD<Node<double[]>, NeighborList> graph =
                JavaPairRDD.fromJavaRDD(tuples);

        Partitioner<double[]> partitioner = getPartitioner((int) budget);
        Partitioning<double[]> partition = partitioner.partition(graph);

        ApproximateSearch<double[]> fast_search = new ApproximateSearch<>(
                partition.graph, new L2Similarity(), 16);

        ExhaustiveSearch<double[]> search = new ExhaustiveSearch<>(
                partition.graph, new L2Similarity());

        int correct = 0;
        int i = 100000;
        for (double[] query : queries) {
            Node<double[]> query_node = new Node("" + i++, query);
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
