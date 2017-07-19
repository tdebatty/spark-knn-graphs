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

import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.spark.knngraphs.builder.Brute;
import info.debatty.spark.knngraphs.builder.DistributedGraphBuilder;
import java.io.IOException;
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
public class KMedoidsPartitionerTest extends TestCase {

    private static final int K = 10;

    /**
     * Test of partition method, of class KMedoidsPartitioner.
     */
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
        JavaPairRDD<Node<String>, NeighborList> graph =
                builder.computeGraph(nodes);
        graph.cache();
        graph.count();

        KMedoidsPartitioner<String> partitioner =
                new KMedoidsPartitioner<String>(new JWSimilarity(), 8);
        partitioner.setBudget(new TimeBudget(10));
        graph = partitioner.partition(graph).graph;
        graph.cache();
        graph.count();

        // Check result...
        System.out.println(JaBeJa.countCrossEdges(graph, 8));
        System.out.println(JaBeJa.computeBalance(graph, 8));

        sc.close();
    }

}
