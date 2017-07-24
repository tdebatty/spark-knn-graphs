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
import info.debatty.jinu.Case;
import info.debatty.spark.knngraphs.builder.Brute;
import info.debatty.spark.knngraphs.eval.JWSimilarity;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
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
public class TestCase {

    private static final int N_TEST = 100;
    private static final Logger LOGGER = Logger.getLogger(TestCase.class);

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception if anything goes wrong
     */
    public static void main(final String[] args) throws Exception {

        // Parse input parameters
        OptionParser parser = new OptionParser("t:r:d:");
        OptionSet options = parser.parse(args);
        List<String> time_list = (List<String>) options.valuesOf("t");
        double[] similarities = new double[time_list.size()];
        for (int i = 0; i < similarities.length; i++) {
            similarities[i] = Double.valueOf(time_list.get(i));
        }

        // Reduce Spark output logs
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        // Read data
        LOGGER.info("Read data...");
        SparkConf conf = new SparkConf();
        conf.setAppName("Spark SPAM search");
        conf.setIfMissing("spark.master", "local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        LinkedList<String> strings = new LinkedList(
                sc
                    .textFile((String) options.valueOf("d"), 16)
                    .collect());

        // Split between main and test data
        Random rand = new Random();
        LinkedList<String> queries = new LinkedList<>();
        for (int i = 0; i < N_TEST; i++) {
            queries.add(strings.remove(rand.nextInt(strings.size())));
        }


        // Build the list of nodes
        LinkedList<Node<String>> nodes = new LinkedList<>();
        int i = 0;
        for (String string : strings) {
            nodes.add(new Node<>(String.valueOf(i), string));
            i++;
        }

        JavaRDD<Node<String>> nodes_rdd = sc.parallelize(nodes);

        // Build the graph
        LOGGER.info("Build graph...");
        Brute<String> brute = new Brute();
        brute.setK(10);
        brute.setSimilarity(new JWSimilarity());
        JavaPairRDD<Node<String>, NeighborList> graph =
                brute.computeGraph(nodes_rdd);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HHmmss");
        Date now = new Date();
        String graph_path = "spam-graph-" + sdf.format(now);
        graph.saveAsObjectFile(graph_path);
        LOGGER.info("Graph saved to " + graph_path);

        // Run the test
        AbstractTest.graph_path = graph_path;
        AbstractTest.queries = queries;
        // KMedoidsTest.graph_path = graph_path;
        // KMedoidsTest.queries = queries;
        // Edg1DTest.graph_path = graph_path;
        // Edge1DTest.queries = queries;

        Case test = new Case();
        test.setDescription(TestCase.class.getName() + " : "
                + String.join(" ", Arrays.asList(args)));
        test.setIterations(20);
        test.setParallelism(1);
        test.commitToGit(false);
        test.setBaseDir((String) options.valueOf("r"));
        test.setParamValues(similarities);

        test.addTest(JaBeJaTest.class);
        test.addTest(KMedoidsTest.class);
        test.addTest(Edge1DTest.class);

        LOGGER.info("Run tests...");
        test.run();

    }
}
