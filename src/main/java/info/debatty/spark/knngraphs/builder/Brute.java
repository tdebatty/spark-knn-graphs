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
import info.debatty.java.graphs.Node;
import java.io.Serializable;
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
public class Brute<T> extends DistributedGraphBuilder<T> implements Serializable {

    @Override
    protected JavaPairRDD<Node<T>, NeighborList> _computeGraph(JavaRDD<Node<T>> nodes) throws Exception {
        JavaPairRDD<Node<T>, Node<T>> pairs = nodes.cartesian(nodes);
        JavaPairRDD<Node<T>, Neighbor> allsimilarities = pairs.mapToPair(new PairFunction<Tuple2<Node<T>, Node<T>>, Node<T>, Neighbor>() {

            public Tuple2<Node<T>, Neighbor> call(Tuple2<Node<T>, Node<T>> tuple) throws Exception {
                return new Tuple2<Node<T>, Neighbor>(
                        tuple._1,
                        new Neighbor(
                                tuple._2,
                                similarity.similarity(tuple._1.value, tuple._2.value)));
            }
        });

        JavaPairRDD<Node<T>, NeighborList> graph = allsimilarities.aggregateByKey(
                new NeighborList(k),
                new  Function2<NeighborList, Neighbor, NeighborList>() {public NeighborList call(NeighborList nl, Neighbor n) throws Exception {
                    nl.add(n);
                    return nl;
                }
                },
                new  Function2<NeighborList, NeighborList, NeighborList>() {

                    public NeighborList call(NeighborList nl1, NeighborList nl2) throws Exception {
                        nl1.addAll(nl2);
                        return nl1;
                    }
                });

        return graph;
    }

}
