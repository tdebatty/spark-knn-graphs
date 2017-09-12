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
package info.debatty.spark.knngraphs;

import info.debatty.java.graphs.Neighbor;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

/**
 * Search the complete graph to find the nearest neighbors of query.
 * @author Thibault Debatty
 * @param <T>
 */
public class ExhaustiveSearch<T> implements Serializable {
    private final JavaPairRDD<Node<T>, NeighborList> graph;
    private final SimilarityInterface<T> similarity;

    /**
     *
     * @param graph
     * @param similarity
     */
    public ExhaustiveSearch(
            final JavaPairRDD<Node<T>, NeighborList> graph,
            final SimilarityInterface<T> similarity) {
        this.graph = graph;
        this.similarity = similarity;
    }

    /**
     * Search the complete graph to find the k nearest neighbors of query.
     * @param query
     * @param k
     * @return
     */
    public final NeighborList search(final Node<T> query, final int k) {
        JavaRDD<NeighborList> candidates_neighborlists =
                graph.mapPartitions(new SearchFunction(k, query, similarity));

        NeighborList final_neighborlist = new NeighborList(k);
        for (NeighborList nl : candidates_neighborlists.collect()) {
            final_neighborlist.addAll(nl);
        }

        return final_neighborlist;
    }
}

/**
 * Searches all partitions in parallel.
 *
 * Each partition returns a neighborlist of k nearest neighbors.
 * @author Thibault Debatty
 * @param <T>
 */
class SearchFunction<T>
        implements FlatMapFunction<
            Iterator<Tuple2<Node<T>, NeighborList>>,
            NeighborList> {

    private final int k;
    private final Node<T> query;
    private final SimilarityInterface<T> similarity;

    SearchFunction(
            final int k,
            final Node<T> query,
            final SimilarityInterface<T> similarity) {

        this.k = k;
        this.query = query;
        this.similarity = similarity;
    }

    public Iterator<NeighborList> call(
            final Iterator<Tuple2<Node<T>, NeighborList>> tuples_iterator) {
        NeighborList local_nl = new NeighborList(k);
        while (tuples_iterator.hasNext()) {
            Node<T> next = tuples_iterator.next()._1;
            local_nl.add(new Neighbor(
                    next,
                    similarity.similarity(query.value, next.value)));
        }

        ArrayList<NeighborList> result = new ArrayList<NeighborList>(1);
        result.add(local_nl);
        return result.iterator();

    }
}
