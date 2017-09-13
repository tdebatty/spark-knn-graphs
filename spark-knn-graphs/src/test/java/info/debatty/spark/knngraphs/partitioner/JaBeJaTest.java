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

package info.debatty.spark.knngraphs.partitioner;

import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.spark.knngraphs.DistributedGraph;
import info.debatty.spark.knngraphs.SparkCase;
import info.debatty.spark.knngraphs.partitioner.jabeja.TimeBudget;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

/**
 *
 * @author Thibault Debatty
 */
public class JaBeJaTest extends SparkCase {

    private static final int PARTITIONS = 8;
    private static final int TIME_BUDGET = 15;

    /**
     * Build the color index.
     */
    public final void testBuildIndex() {
        System.out.println("BuildIndex");
        System.out.println("==========");

        JavaPairRDD<Node<String>, NeighborList> graph = readSpamGraph();

        JaBeJa<String> jbj = new JaBeJa<String>(PARTITIONS);
        graph = jbj.randomize(graph);
        graph.cache();
        Tuple2<Node<String>, NeighborList> first = graph.first();
        int first_partition = (Integer) first._1
                .getAttribute(NodePartitioner.PARTITION_KEY);
        int first_id = Integer.valueOf(first._1.id);

        int[] index = JaBeJa.buildColorIndex(graph);
        assertEquals(first_partition, index[first_id]);
    }

    /**
     * Perform a single swap and check the number of cross-partition edges
     * decreases.
     */
    public final void testSwap() {
        System.out.println("Swap");
        System.out.println("====");

        JavaPairRDD<Node<String>, NeighborList> graph = readSpamGraph();

        JaBeJa<String> jbj = new JaBeJa<String>(PARTITIONS);

        // Randomize
        graph = jbj.randomize(graph);
        graph = DistributedGraph.moveNodes(graph, PARTITIONS);
        graph.cache();

        testPartitionNotNull(graph);

        int cross_edges_before = JaBeJa.countCrossEdges(graph, PARTITIONS);

        // Perform Swap
        graph = jbj.swap(graph, 2.0, 1).graph;
        graph = DistributedGraph.moveNodes(graph, PARTITIONS);
        graph.cache();
        graph.count();
        testPartitionNotNull(graph);

        int cross_edges_after = JaBeJa.countCrossEdges(graph, PARTITIONS);

        assertTrue("Number of cross edges should decrease!",
                cross_edges_after < cross_edges_before);

    }

    /**
     * Run for a fixed time.
     */
    public final void testTimeBudget() {
        System.out.println("TimeBudget");
        System.out.println("==========");

        JavaPairRDD<Node<String>, NeighborList> graph = readSpamGraph();

        JaBeJa<String> jbj = new JaBeJa<String>(
                PARTITIONS,
                new TimeBudget(TIME_BUDGET));
        Partitioning<String> solution = jbj.partition(graph);
        System.out.println(JaBeJa.countCrossEdges(solution.graph, PARTITIONS));
        testPartitionNotNull(solution.graph);
        assertEquals(TIME_BUDGET, solution.runTime() / 1000);
    }

    /**
     * Test that every node has a non null partition value.
     * @param graph
     */
    private void testPartitionNotNull(
            final JavaPairRDD<Node<String>, NeighborList> graph) {

        for (Tuple2<Node<String>, NeighborList> tuple : graph.collect()) {
            assertNotNull("Partition id is null!", tuple._1.getAttribute(
                    NodePartitioner.PARTITION_KEY));
        }

    }

}
