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
package info.debatty.spark.knngraphs.builder;

import info.debatty.java.graphs.NeighborList;
import info.debatty.spark.knngraphs.JWSimilarity;
import info.debatty.spark.knngraphs.KNNGraphCase;
import info.debatty.spark.knngraphs.Node;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

/**
 *
 * @author tibo
 */
public class NNCTPHTest extends KNNGraphCase {

    /**
     *
     * @throws Exception cannot read dataset or build graph
     */
    public final void testSpamGraph() throws Exception {
        JavaRDD<String> nodes = readSpam();

        NNCTPH builder = new NNCTPH();
        builder.setBuckets(20);
        builder.setStages(3);
        builder.setK(10);
        builder.setSimilarity(new JWSimilarity());

        JavaPairRDD<Node<String>, NeighborList> graph =
                builder.computeGraph(nodes);
        graph.count();
    }
}
