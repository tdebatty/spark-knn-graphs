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

import info.debatty.java.graphs.Neighbor;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.spark.knngraphs.Node;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 *
 * @author Thibault Debatty
 * @param <T>
 */
public class Brute<T> extends DistributedGraphBuilder<T> {

    @Override
    protected final JavaPairRDD<Node<T>, NeighborList> doComputeGraph(
            final JavaRDD<Node<T>> nodes) {

        JavaPairRDD<Node<T>, Node<T>> pairs = nodes.cartesian(nodes);

        // Compute all pairwize similarities
        // Similarity is stored in a Neighbor object
        JavaPairRDD<Node<T>, Neighbor> allsimilarities = pairs.mapToPair(
                new PairwizeSimilarityFunction(similarity));

        JavaPairRDD<Node<T>, NeighborList> graph =
                allsimilarities.aggregateByKey(

                    // Init : empty neighborlist
                    new NeighborList(k),

                    // Add neighbor to neighborlist
                    new  AggregateNeighborsFunction(),

                    // Combine neighborlists
                    new  CombineNeighborListsFunction());

        return graph;
    }

}

class PairwizeSimilarityFunction<T>
        implements PairFunction<Tuple2<Node<T>, Node<T>>, Node<T>, Neighbor> {

    private final SimilarityInterface<T> similarity;

    PairwizeSimilarityFunction(final SimilarityInterface<T> similarity) {
        this.similarity = similarity;
    }

    @Override
    public Tuple2<Node<T>, Neighbor> call(
            final Tuple2<Node<T>, Node<T>> nodes_pair) {

        // Compute the similarity only if the nodes are different
        // to avoid a node receives himself as neighbor...
        double sim = 0;
        if (!nodes_pair._1.equals(nodes_pair._2)) {
            sim = similarity.similarity(
                    nodes_pair._1.value,
                    nodes_pair._2.value);
        }

        return new Tuple2<>(
                nodes_pair._1,
                new Neighbor(nodes_pair._2, sim));
    }
}

class AggregateNeighborsFunction
        implements Function2<NeighborList, Neighbor, NeighborList> {

    @Override
    public NeighborList call(
            final NeighborList nl,
            final Neighbor n) {
        nl.add(n);
        return nl;
    }
}

class CombineNeighborListsFunction
        implements Function2<NeighborList, NeighborList, NeighborList> {

    @Override
    public NeighborList call(
            final NeighborList nl1,
            final NeighborList nl2) {
        nl1.addAll(nl2);
        return nl1;
    }
}