/*
 * The MIT License
 *
 * Copyright 2017 Thibault Debatty.
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
import scala.Tuple2;

/**
 * A base class for spark test cases. Contains no test...
 * @author Thibault Debatty
 */
public class SparkCase extends TestCase {

    private JavaSparkContext sc = null;

    /**
     * Get SparkContext.
     * @return
     */
    protected final JavaSparkContext getSpark() {
        return sc;
    }

    @Override
    protected final void setUp() throws Exception {
        super.setUp();

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setIfMissing("spark.master", "local[*]");
        sc = new JavaSparkContext(conf);
    }

    @Override
    protected final void tearDown() throws Exception {
        if (sc != null) {
            sc.close();
        }
        super.tearDown();
    }

    /**
     * Read the SPAM dataset from resources and parallelize in Spark.
     * @return
     * @throws IOException if we cannot read the file
     */
    public final JavaRDD<Node<String>> readSpam() throws IOException {
        String file =  getClass().getClassLoader().
                getResource("726-unique-spams").getPath();

        // Read the file
        ArrayList<String> strings = DistributedGraphBuilder.readFile(file);

        // Convert to nodes
        List<Node<String>> data = new ArrayList<Node<String>>();
        for (String s : strings) {
            data.add(new Node<String>(String.valueOf(data.size()), s));
        }

        // Parallelize the dataset in Spark
        JavaRDD<Node<String>> nodes = sc.parallelize(data);
        return nodes;
    }

    /**
     * Read the exact SPAM graph from resources.
     * @return
     */
    public final JavaPairRDD<Node<String>, NeighborList> readSpamGraph() {
        String file =  getClass().getClassLoader().
                getResource("graph-spam").getPath();

        JavaRDD<Tuple2<Node<String>, NeighborList>> tuples =
                sc.objectFile(file, 8);
        JavaPairRDD<Node<String>, NeighborList> graph =
                JavaPairRDD.fromJavaRDD(tuples);

        return graph;
    }

    /**
     * Read the exact synthetic graph from resources.
     * @return
     */
    public final JavaPairRDD<Node<double[]>, NeighborList>
        readSyntheticGraph() {

        String file =  getClass().getClassLoader().
                getResource("graph-synthetic-10K").getPath();

        JavaRDD<Tuple2<Node<double[]>, NeighborList>> tuples =
                sc.objectFile(file, 8);
        JavaPairRDD<Node<double[]>, NeighborList> graph =
                JavaPairRDD.fromJavaRDD(tuples);

        return graph;
    }
}
