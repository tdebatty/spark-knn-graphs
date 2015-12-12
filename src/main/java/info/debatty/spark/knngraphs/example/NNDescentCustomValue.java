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

import info.debatty.java.graphs.Neighbor;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.spark.knngraphs.builder.NNDescent;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

/**
 * In this example, a custom class is used as node value.
 * The only requirement is that the custom class must implement Serializable.
 * 
 * This also allows to support the use case where you have, say, user vectors, 
 * and item vectors. And you'd like to compute the NN for item-item and the 
 * user-item similarity.
 * 
 * @author Thibault Debatty
 */
public class NNDescentCustomValue {

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        // Configure spark instance
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkTest");
        conf.setIfMissing("spark.master", "local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // Create some nodes
        // the value of the nodes will a custom class
        List<Node<CustomValue>> data = new ArrayList<Node<CustomValue>>();
        Random rand = new Random();
        
        // Let's add some values of type 1 ...
        for (int i = 0; i < 1000; i++) {
            CustomValue value = new CustomValue(
                    CustomValue.TYPE1, rand.nextDouble() * 100);
            data.add(new Node("TYPE1_" + String.valueOf(i), value));
        }
        
        // ... and some values of type 2
        for (int i = 0 ; i < 1000; i++) {
            CustomValue value = new CustomValue(
                    CustomValue.TYPE2, 
                    rand.nextDouble() * 100);
            data.add(new Node("TYPE2_" + String.valueOf(i), value));
        }
        JavaRDD<Node<CustomValue>> nodes = sc.parallelize(data);
        
        // Instanciate and configure NNDescent for Integer node values
        NNDescent nndes = new NNDescent<CustomValue>();
        nndes.setK(10);
        nndes.setMaxIterations(10);
        nndes.setSimilarity(new SimilarityInterface<CustomValue>() {

            // Define the similarity that will be used
            // in this case: 1 / (1 + delta)
            public double similarity(CustomValue value1, CustomValue value2) {
                // This is specific !!
                // We only wish to compute similarities between:
                // - type1 and type1 or
                // - type1 and type2
                // .. but we are not interested in similarities betwteen
                // type2 and type2
                if (value1.type == CustomValue.TYPE2 &&
                        value2.type == CustomValue.TYPE2) {
                    return -1;
                }

                // The value of nodes is an integer...
                return 1.0 / (1.0 + Math.abs(value1.value - value2.value));
            }
        });
        
        // Compute the graph...
        JavaPairRDD<Node, NeighborList> graph = nndes.computeGraph(nodes);

        // BTW: until now graph is only an execution plan and nothing has been
        // executed by the spark cluster...
        // This will actually compute the graph...
        double total_similarity = graph.aggregate(
                0.0,
                new  Function2<Double,Tuple2<Node,NeighborList>,Double>() {

                    public Double call(
                            Double val, 
                            Tuple2<Node, NeighborList> tuple) throws Exception {
                        for (Neighbor n : tuple._2()) {
                            val += n.similarity;
                        }

                        return val;
                    }
                },
                new Function2<Double, Double, Double>() {

                    public Double call(
                            Double val0, 
                            Double val1) throws Exception {
                        return val0 + val1;
                    }

                });

        System.out.println("Total sim: " + total_similarity);
        System.out.println(graph.first());
    }
    
}

class CustomValue implements Serializable
{
    public static int TYPE1 = 1;
    public static int TYPE2 = 2;
    
    public int type;
    public double value;
    
    public CustomValue(int type, double value) {
        this.type = type;
        this.value = value;
    }
}