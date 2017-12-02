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
package partitioning.synthetic;

import info.debatty.java.graphs.NeighborList;

import info.debatty.jinu.TestInterface;
import info.debatty.spark.knngraphs.partitioner.JaBeJa;
import info.debatty.spark.knngraphs.partitioner.KMedoids;
import info.debatty.spark.knngraphs.partitioner.Partitioning;
import info.debatty.spark.kmedoids.budget.TimeBudget;
import info.debatty.spark.knngraphs.Node;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author tibo
 */
public class KMedoidsTest implements TestInterface {

    private static final double IMBALANCE = 1.2;

    public static String dataset_path;

    @Override
    public final double[] run(final double budget) throws Exception {

        SparkConf conf = new SparkConf();
        conf.setAppName("Spark graph partitioning with SPAM");
        conf.setIfMissing("spark.master", "local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Tuple2<Node<double[]>, NeighborList>> tuples =
                sc.objectFile(dataset_path);
        JavaPairRDD<Node<double[]>, NeighborList> graph =
                JavaPairRDD.fromJavaRDD(tuples);

        KMedoids<double[]> partitioner =
                new KMedoids<>(
                        new L2Similarity(),
                        16,
                        IMBALANCE,
                        new TimeBudget((int) budget));
        Partitioning<double[]> partition = partitioner.partition(graph);
        double[] result = new double[] {
            JaBeJa.countCrossEdges(partition.wrapped_graph, 16),
            JaBeJa.computeBalance(partition.wrapped_graph, 16)
        };
        sc.close();

        return result;
    }
}
