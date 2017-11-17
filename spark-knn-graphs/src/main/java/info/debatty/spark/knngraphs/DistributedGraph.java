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

import info.debatty.java.graphs.Graph;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.spark.knngraphs.builder.NodeSimilarityAdapter;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

/**
 *
 * @author Thibault Debatty
 */
public class DistributedGraph {

    /**
     * Wrap the nodes with a node class (which decorates with an id
     * and partition).
     * @param <T>
     * @param nodes
     * @return
     */
    public static final <T> JavaRDD<Node<T>> wrapNodes(
            final JavaRDD<T> nodes) {

        return nodes.zipWithUniqueId().map(new WrapNodeFunction());

    }

    /**
     *
     * @param <T>
     * @param graph1
     * @param graph2
     * @return
     */
    public static final <T> long countCommonEdges(
            final JavaPairRDD<Node<T>, NeighborList> graph1,
            final JavaPairRDD<Node<T>, NeighborList> graph2) {

        return (long) graph1
                .union(graph2)
                .groupByKey()
                .map(new CountFunction())
                .reduce(new SumFunction());
    }

    /**
     * Convert a PairRDD of (Node, NeighborList) to a RDD of Graph.
     * @param <T>
     * @param graph
     * @param similarity
     * @return
     */
    public static final <T> JavaRDD<Graph<Node<T>>> toGraph(
            final JavaPairRDD<Node<T>, NeighborList> graph,
            final SimilarityInterface<T> similarity) {
        return graph.mapPartitions(
                new NeighborListToGraph(similarity), true);
    }
}

class WrapNodeFunction<T> implements Function<Tuple2<T, Long>, Node<T>> {

    @Override
    public Node<T> call(
            final Tuple2<T, Long> value) {

        Node<T> node = new Node<>();
        node.id = value._2;
        node.value = value._1;

        return node;
    }
}

/**
 * Used to convert a PairRDD Node,NeighborList to a RDD of Graph.
 * @author Thibault Debatty
 * @param <T>
 */
class NeighborListToGraph<T>
        implements FlatMapFunction<
            Iterator<Tuple2<Node<T>, NeighborList>>, Graph<Node<T>>> {

    private final SimilarityInterface<T> similarity;

    NeighborListToGraph(final SimilarityInterface<T> similarity) {

        this.similarity = similarity;
    }

    @Override
    public Iterator<Graph<Node<T>>> call(
            final Iterator<Tuple2<Node<T>, NeighborList>> iterator) {

        info.debatty.java.graphs.Graph<Node<T>> graph = new Graph<>();
        while (iterator.hasNext()) {
            Tuple2<Node<T>, NeighborList> next = iterator.next();
            graph.put(next._1, next._2);
        }

        graph.setSimilarity(new NodeSimilarityAdapter<>(similarity));
        graph.setK(
                graph.getNeighbors(graph.getNodes().iterator().next()).size());

        ArrayList<Graph<Node<T>>> list = new ArrayList<>(1);
        list.add(graph);
        return list.iterator();

    }
}

/**
 *
 * @author Thibault Debatty
 * @param <T>
 */
class CountFunction<T>
        implements Function<Tuple2<T, Iterable<NeighborList>>, Long> {

    /**
     *
     * @param tuples
     * @return
     */
    @Override
    public Long call(final Tuple2<T, Iterable<NeighborList>> tuples) {
        Iterator<NeighborList> iterator = tuples._2.iterator();
        NeighborList nl1 = iterator.next();
        NeighborList nl2 = iterator.next();
        return new Long(nl1.countCommons(nl2));
    }

}

/**
 *
 * @author Thibault Debatty
 */
class SumFunction
        implements Function2<Long, Long, Long> {

    /**
     *
     * @param arg0
     * @param arg1
     * @return
     * @throws Exception
     */
    @Override
    public Long call(final Long arg0, final Long arg1) {
        return arg0 + arg1;
    }

}