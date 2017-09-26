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
package jbj.commercials;

import info.debatty.java.datasets.tv.Sequence;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.jinu.TestInterface;
import info.debatty.spark.knngraphs.partitioner.JaBeJa;
import info.debatty.spark.knngraphs.partitioner.Partitioning;
import info.debatty.spark.knngraphs.partitioner.jabeja.TimeBudget;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Run JaBeJa partitioning until convergence...
 * @author tibo
 */
public class JaBeJaTest implements TestInterface {

    public static String dataset_path;

    @Override
    public final double[] run(final double time) throws Exception {

        SparkConf conf = new SparkConf();
        conf.setAppName("Spark graph partitioning with Commercials");
        conf.setIfMissing("spark.master", "local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read graph from HDFS
        JavaRDD<Tuple2<Node<Sequence>, NeighborList>> tuples =
                sc.objectFile(dataset_path);
        JavaPairRDD<Node<Sequence>, NeighborList> graph =
                JavaPairRDD.fromJavaRDD(tuples);

        // Partition
        JaBeJa<Sequence> partitioner = new JaBeJa<>(
                16, new TimeBudget((int) time));
        Partitioning<Sequence> partition = partitioner.partition(graph);

        // Check result
        double[] result = new double[] {
            JaBeJa.countCrossEdges(partition.graph, 16),
            JaBeJa.computeBalance(partition.graph, 16)
        };
        sc.close();

        return result;
    }
}
