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
package info.debatty.spark.knngraphs;

import info.debatty.java.datasets.gaussian.Dataset;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.spark.knngraphs.builder.Brute;
import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author tibo
 */
public class Edge1DPartitionerTest extends TestCase {

    public void testPartition() throws Exception {
        System.out.println("Partition");
        System.out.println("=========");

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        Dataset dataset = new Dataset.Builder(10, 13)
                .setOverlap(Dataset.Builder.Overlap.HIGH)
                .setSize(2000)
                .build();

        // Convert to nodes
        List<Node<double[]>> data = new ArrayList<Node<double[]>>();
        for (double[] point : dataset) {
            data.add(new Node<double[]>(String.valueOf(data.size()), point));
        }

        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setIfMissing("spark.master", "local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Parallelize the dataset in Spark
        JavaRDD<Node<double[]>> nodes = sc.parallelize(data);

        // Build the graph
        Brute<double[]> builder = new Brute<double[]>();
        builder.setK(10);
        builder.setSimilarity(new L2Similarity());
        JavaPairRDD<Node<double[]>, NeighborList> graph =
                builder.computeGraph(nodes);

        graph.repartition(8);
        graph.cache();
        graph.count();

        // Partition
        Edge1DPartitioner<double[]> partitioner =
                new Edge1DPartitioner<double[]>(8);
        graph = partitioner.partition(graph).graph;
        graph.cache();
        graph.count();

        // Check result...
        System.out.println("Cross-partition edges: "
                + JaBeJa.countCrossEdges(graph, 8));
        double imbalance = JaBeJa.computeBalance(graph, 8);
        System.out.println("Imbalance: " + imbalance);

        sc.close();
    }


}
