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
package partitioning.spam;

import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.spark.knngraphs.builder.Brute;
import info.debatty.spark.knngraphs.eval.JWSimilarity;
import java.util.Iterator;
import java.util.LinkedList;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 *
 * @author tibo
 */
public class BuildGraph {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        OptionParser parser = new OptionParser("i:o:");
        OptionSet options = parser.parse(args);
        String dataset_path = (String) options.valueOf("i");
        String output_path = (String) options.valueOf("o");

        SparkConf conf = new SparkConf();
        conf.setAppName("Spark build SPAM graph");
        conf.setIfMissing("spark.master", "local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> data = sc.textFile(dataset_path, 16);

        // Convert to nodes
        JavaRDD<Node<String>> nodes =
                data.mapPartitionsWithIndex(new StringToNodeFunction(), true);

        Brute<String> brute = new Brute();
        brute.setK(10);
        brute.setSimilarity(new JWSimilarity());
        JavaPairRDD<Node<String>, NeighborList> graph =
                brute.computeGraph(nodes);

        graph.saveAsObjectFile(output_path);

    }

}

// implements
class StringToNodeFunction
        implements Function2<Integer, Iterator<String>, Iterator<Node<String>>> {

    private static final long OFFSET = 1000000;

    public Iterator<Node<String>> call(
            final Integer partition, final Iterator<String> strings) {

        LinkedList<Node<String>> nodes = new LinkedList<Node<String>>();
        int i = 0;

        while (strings.hasNext()) {
            nodes.add(new Node<String>(
                    String.valueOf(partition * OFFSET + i),
                    strings.next()));
            i++;
        }

        return nodes.iterator();
    }

}