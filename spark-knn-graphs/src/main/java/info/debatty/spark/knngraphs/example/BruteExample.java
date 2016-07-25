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

package info.debatty.spark.knngraphs.example;

import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.java.stringsimilarity.JaroWinkler;
import info.debatty.spark.knngraphs.builder.Brute;
import info.debatty.spark.knngraphs.builder.DistributedGraphBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author Thibault Debatty
 */
public class BruteExample {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException, Exception {
        if (args.length != 1) {
            System.out.println(
                    "Usage: spark-submit --class " +
                    Search.class.getCanonicalName() + " " +
                    "<dataset>");
        }
        
        String file =  args[0];
        
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
        
        // Compute the graph...
        JavaPairRDD<Node<String>, NeighborList> graph = 
                brute.computeGraph(nodes);
        System.out.println(graph.first());
    }
    
}
