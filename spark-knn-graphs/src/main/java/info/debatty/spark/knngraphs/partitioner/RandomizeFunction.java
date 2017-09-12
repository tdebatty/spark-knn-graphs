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

package info.debatty.spark.knngraphs.partitioner;

import info.debatty.spark.knngraphs.partitioner.NodePartitioner;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import java.util.Random;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Randomize dataset by assigning a random value to each node (in attribute
 * NodePartition.PARTITION_KEY).
 *
 * Usage:
 * graph = graph.mapToPair(new RandomizeFunction(partitions))
 *              .partitionBy(new NodePartitioner(partitions));
 * @author Thibault Debatty
 * @param <T> type of data to process
 */
class RandomizeFunction<T>
        implements PairFunction<
            Tuple2<Node<T>, NeighborList>,
            Node<T>,
            NeighborList> {

    private final int partitions;
    private Random rand;

    RandomizeFunction(final int partitions) {
        this.partitions = partitions;
    }

    public Tuple2<Node<T>, NeighborList> call(
            final Tuple2<Node<T>, NeighborList> tuple) {

        if (rand == null) {
            rand = new Random();
        }

        tuple._1.setAttribute(
                NodePartitioner.PARTITION_KEY,
                rand.nextInt(partitions));

        return tuple;
    }

}
