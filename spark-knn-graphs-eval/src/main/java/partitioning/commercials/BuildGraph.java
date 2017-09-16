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
package partitioning.commercials;

import info.debatty.java.datasets.tv.Sequence;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.spark.knngraphs.builder.Brute;
import java.util.LinkedList;
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
public class BuildGraph {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        OptionParser parser = new OptionParser("i:o:");
        OptionSet options = parser.parse(args);
        String dataset_path = (String) options.valueOf("i");
        String output_path = (String) options.valueOf("o");

        SparkConf conf = new SparkConf();
        conf.setAppName("Spark build Synthetic graph");
        conf.setIfMissing("spark.master", "local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> strings = sc.textFile(dataset_path);

        // Parse TV sequences
        JavaRDD<Sequence> sequences =
                strings.map(new ParseSequenceFunction());
        sequences.cache();
        sequences.count();

        LinkedList<Node<Sequence>> nodes = new LinkedList<>();
        int i = 0;
        for (Sequence value : sequences.collect()) {
            nodes.add(new Node<>(String.valueOf(i), value));
            i++;
        }

        JavaRDD<Node<Sequence>> nodes_rdd = sc.parallelize(nodes);

        Brute<Sequence> brute = new Brute<>();
        brute.setK(10);
        brute.setSimilarity(new SequenceSimilarity());
        JavaPairRDD<Node<Sequence>, NeighborList> graph =
                brute.computeGraph(nodes_rdd);

        graph.saveAsObjectFile(output_path);

    }
}
