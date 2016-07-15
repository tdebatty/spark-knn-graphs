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

package info.debatty.spark.knngraphs.builder;

import info.debatty.java.graphs.Neighbor;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.java.stringsimilarity.JaroWinkler;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author Thibault Debatty
 */
public class BruteTest extends TestCase implements Serializable {

    public BruteTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testComputeGraph() throws IOException, Exception {

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

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

        Brute brute = new Brute();
        brute.setK(10);
        brute.setSimilarity(new SimilarityInterface<String>() {

            public double similarity(String value1, String value2) {
                JaroWinkler jw = new JaroWinkler();
                return jw.similarity(value1, value2);
            }
        });

        // Compute the graph and force execution
        JavaPairRDD<Node<String>, NeighborList> graph = brute.computeGraph(nodes);
        graph.first();
        List<Tuple2<Node<String>, NeighborList>> local_graph = graph.collect();

        sc.close();


        // Check wether a node receives himself as neighbor...
        for (Tuple2<Node<String>, NeighborList> tuple : local_graph) {
            Node<String> node = tuple._1;
            for (Neighbor neighbor : tuple._2) {
                assertTrue(!node.equals(neighbor.node));
            }
        }
    }

}
