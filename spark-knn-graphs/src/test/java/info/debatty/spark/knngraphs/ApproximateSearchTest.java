/*
 * The MIT License
 *
 * Copyright 2016 Thibault Debatty.
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

package info.debatty.spark.knngraphs;

import info.debatty.java.datasets.gaussian.Dataset;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import java.util.Iterator;
import org.apache.spark.api.java.JavaPairRDD;

/**
 *
 * @author Thibault Debatty
 */
public class ApproximateSearchTest extends SparkCase {

    private static final int N_TEST = 100;
    private static final int N_CORRECT = 50;
    private static final int DIMENSIONALITY = 10;
    private static final int NUM_CENTERS = 3;
    private static final double SPEEDUP = 4;
    private static final int PARTITIONS = 8;

    /**
     * Test of search method, of class ApproximateSearch.
     */
    public final void testSearch() {
        System.out.println("Search");
        System.out.println("======");

        JavaPairRDD<Node<double[]>, NeighborList> graph = readSyntheticGraph();

        ExhaustiveSearch<double[]> exhaustive_search =
                new ExhaustiveSearch<double[]>(graph, new L2Similarity());

        System.out.println("Prepare the graph for approximate search");
        ApproximateSearch<double[]> approximate_search =
                new ApproximateSearch<double[]>(
                        graph,
                        new L2Similarity(),
                        PARTITIONS);

        System.out.println("Perform some search queries...");

        Iterator<double[]> queryies
                = new Dataset.Builder(DIMENSIONALITY, NUM_CENTERS)
                .setOverlap(Dataset.Builder.Overlap.MEDIUM)
                .build()
                .iterator();
        int correct = 0;
        for (int i = 0; i < N_TEST; i++) {
            double[] point = queryies.next();
            Node<double[]> query =
                    new Node<double[]>(
                            "123456789",
                            point);

            NeighborList approximate_result = approximate_search.search(
                    query,
                    1,
                    null,
                    SPEEDUP,
                    ApproximateSearch.DEFAULT_JUMPS,
                    ApproximateSearch.DEFAULT_EXPANSION);

            NeighborList exhaustive_result = exhaustive_search.search(query, 1);
            correct += approximate_result.countCommons(exhaustive_result);
        }

        System.out.println("Found " + correct + " correct search results");

        assertTrue(
                "Not enough correct search results: " + correct,
                correct > N_CORRECT);
    }
}
