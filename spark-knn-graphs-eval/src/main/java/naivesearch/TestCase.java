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
package naivesearch;

import info.debatty.java.graphs.NeighborList;

import info.debatty.jinu.Case;
import info.debatty.jinu.TestFactory;
import info.debatty.jinu.TestInterface;
import info.debatty.spark.knngraphs.Node;
import info.debatty.spark.knngraphs.builder.Brute;
import info.debatty.spark.knngraphs.eval.L2Similarity;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
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
        // -t time to partition the graph (1 or more values)
        // -r directory to write result files
        // -d path to dataset
        OptionParser parser = new OptionParser("t:r:d:");
        OptionSet options = parser.parse(args);
        List<String> time_list = (List<String>) options.valuesOf("t");
        double[] time_array = new double[time_list.size()];
        for (int i = 0; i < time_array.length; i++) {
            time_array[i] = Double.valueOf(time_list.get(i));
        }

        // Read data
        LOGGER.info("Read data...");
        SparkConf conf = new SparkConf();
        conf.setAppName("Spark synthetic (naive) search");
        conf.setIfMissing("spark.master", "local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        LinkedList<double[]> vectors = new LinkedList(
                sc
                    .objectFile((String) options.valueOf("d"), 8)
                    .collect());

        // Split between main and test data
        Random rand = new Random();
        LinkedList<double[]> queries = new LinkedList<>();
        for (int i = 0; i < N_TEST; i++) {
            queries.add(vectors.remove(rand.nextInt(vectors.size())));
        }

        JavaRDD<double[]> nodes_rdd = sc.parallelize(vectors);

        // Build the graph
        LOGGER.info("Build graph...");
        Brute<double[]> brute = new Brute();
        brute.setK(10);
        brute.setSimilarity(new L2Similarity());
        JavaPairRDD<Node<double[]>, NeighborList> graph =
                brute.computeGraph(nodes_rdd);

        // Save the graph to disk
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HHmmss");
        Date now = new Date();
        String graph_path = "synthetic-graph-" + sdf.format(now);
        graph.saveAsObjectFile(graph_path);
        LOGGER.info("Graph saved to " + graph_path);
        sc.close();

        // Run the test
        AbstractTest.graph_path = graph_path;
        AbstractTest.queries = queries;

        Case test = new SparkCase();
        test.setDescription(TestCase.class.getName() + " : "
                + String.join(" ", Arrays.asList(args)));

        test.setBaseDir((String) options.valueOf("r"));
        test.setParamValues(time_array);

        test.addTest(new TestFactory() {
            @Override
            public TestInterface newInstance() {
                return new FastSearch();
            }
        });

        test.addTest(new TestFactory() {
            @Override
            public TestInterface newInstance() {
                return new NaiveSearch();
            }
        });

        LOGGER.info("Run tests...");
        test.run();
    }
}
