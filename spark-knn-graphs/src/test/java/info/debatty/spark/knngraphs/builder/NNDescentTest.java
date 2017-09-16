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

import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.spark.knngraphs.DistributedGraph;
import info.debatty.spark.knngraphs.JWSimilarity;
import info.debatty.spark.knngraphs.SparkCase;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

/**
 *
 * @author Thibault Debatty
 */
public class NNDescentTest extends SparkCase {

    private static final int K = 10;

    /**
     * Test of computeGraph method, of class NNDescent.
     * @throws java.lang.Exception if we cannot build the graph
     */
    public final void testComputeGraph() throws Exception {
        System.out.println("NNDescent");
        System.out.println("=========");

        JavaRDD<Node<String>> nodes = readSpam();

        NNDescent builder = new NNDescent();
        builder.setK(K);
        builder.setSimilarity(new JWSimilarity());
        builder.setMaxIterations(5);

        // Compute the graph and force execution
        JavaPairRDD<Node<String>, NeighborList> graph =
                builder.computeGraph(nodes);
        graph.cache();

        // Perform tests
        assertEquals(nodes.count(), graph.count());
        assertEquals(K, graph.first()._2.size());

        JavaPairRDD<Node<String>, NeighborList> exact_graph = readSpamGraph();
        long correct_edges = DistributedGraph.countCommonEdges(
                exact_graph, graph);

        int correct_threshold = (int) (nodes.count() * K * 0.9);

        assertTrue(
                "Not enough correct edges: " + correct_edges,
                correct_edges >= correct_threshold);
    }
}
