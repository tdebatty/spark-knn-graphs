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
package info.debatty.spark;

import java.util.LinkedList;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 *
 * @author tibo
 */
public class GraphXTest extends SparkCase {
    public void testGraphX() {
        JavaSparkContext spark = getSpark();


        LinkedList<Tuple2<Object, String>> nodes =
                new LinkedList<>();
        nodes.add(new Tuple2<>((Object) 0L, "0"));
        nodes.add(new Tuple2<>((Object) 1L, "1"));
        RDD<Tuple2<Object, String>> nodes_rdd = spark.parallelize(nodes).rdd();

        LinkedList<Edge<String>> edges = new LinkedList<>();
        edges.add(new Edge<>(0L, 1L, ""));
        RDD<Edge<String>> edges_rdd = spark.parallelize(edges).rdd();

        String default_node = "Default";

        StorageLevel storage1 = StorageLevel.MEMORY_ONLY();
        StorageLevel storage2 = StorageLevel.MEMORY_ONLY();

        ClassTag<String> string_class_tag =
                scala.reflect.ClassManifestFactory.fromClass(String.class);

        Graph<String, String> graph = Graph.apply(
                nodes_rdd,
                edges_rdd,
                default_node,
                storage1,
                storage2,
                string_class_tag,
                string_class_tag);

        System.out.println(graph.edges().collect());

        Graph<Object, String> connected_components =
                graph.ops().connectedComponents();
        System.out.println(connected_components.vertices().collect());
    }
}
