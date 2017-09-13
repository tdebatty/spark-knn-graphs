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
package info.debatty.spark.knngraphs.partitioner;

import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.spark.knngraphs.SparkCase;
import org.apache.spark.api.java.JavaPairRDD;

/**
 *
 * @author Thibault Debatty
 */
public class Edge1DTest extends SparkCase {

    private static final int PARTITIONS = 8;
    private static final int K = 10;

    /**
     * Partition synthetic graph with Edge1D and test cross partitions and
     * imbalance.
     */
    public final void testPartition() {
        System.out.println("Partition");
        System.out.println("=========");

        JavaPairRDD<Node<double[]>, NeighborList> graph = readSyntheticGraph();

        // Partition
        Edge1D<double[]> partitioner =
                new Edge1D<double[]>(PARTITIONS);
        graph = partitioner.partition(graph).graph;
        graph.cache();
        graph.count();

        // Check result...
        long random_cross_edges = graph.count() * K
                * (PARTITIONS - 1) / PARTITIONS;
        int cross_edges = JaBeJa.countCrossEdges(graph, PARTITIONS);

        assertEquals(
                "Incorrect number of cross edges!",
                random_cross_edges,
                cross_edges,
                0.001 * random_cross_edges);

        double imbalance = JaBeJa.computeBalance(graph, PARTITIONS);
        assertEquals(
                "Imbalance must be ~ 1.0",
                1.0,
                imbalance,
                0.0001);
    }


}
