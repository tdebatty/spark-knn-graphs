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
import info.debatty.spark.knngraphs.JWSimilarity;
import info.debatty.spark.knngraphs.L2Similarity;
import info.debatty.spark.knngraphs.SparkCase;
import org.apache.spark.api.java.JavaPairRDD;

/**
 *
 * @author Thibault Debatty
 */
public class KMedoidsTest extends SparkCase {

    private static final int K = 10;
    private static final int PARTITIONS = 8;
    private static final double IMBALANCE = 1.2;

    /**
     * Test of partition method, of class KMedoidsPartitioner.
     */
    public final void testPartition() {
        System.out.println("Partition");
        System.out.println("=========");

        JavaPairRDD<Node<String>, NeighborList> graph = readSpamGraph();

        KMedoids<String> partitioner
                = new KMedoids<String>(
                        new JWSimilarity(), PARTITIONS);
        graph = partitioner.partition(graph).graph;
        graph.cache();
        graph.count();

        // Check result...
        int cross_edges = JaBeJa.countCrossEdges(graph, PARTITIONS);
        long random_cross_edges = graph.count() * K
                * (PARTITIONS - 1) / PARTITIONS;

        assertTrue(cross_edges < random_cross_edges);
    }

    /**
     * Partition and test the imbalance remains below threshold.
     */
    public final void testImbalance() {
        System.out.println("Imbalance");
        System.out.println("=========");

        JavaPairRDD<Node<double[]>, NeighborList> graph = readSyntheticGraph();

        // Partition
        KMedoids<double[]> partitioner
                = new KMedoids<double[]>(
                        new L2Similarity(),
                        PARTITIONS,
                        IMBALANCE);
        graph = partitioner.partition(graph).graph;
        graph.cache();
        graph.count();
        // Check result...
        System.out.println(JaBeJa.countCrossEdges(graph, PARTITIONS));
        double imbalance = JaBeJa.computeBalance(graph, PARTITIONS);
        assertTrue("Imbalance failed!", imbalance <= IMBALANCE);
    }
}
