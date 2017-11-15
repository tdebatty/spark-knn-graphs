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

import info.debatty.java.spamsum.ESSum;
import info.debatty.spark.knngraphs.Node;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

/**
 *
 * @author Thibault Debatty
 */
public class NNCTPH extends AbstractPartitioningBuilder<String> {

    @Override
    protected JavaPairRDD<Integer, Node<String>> bin(
            final JavaRDD<Node<String>> nodes) {

        return nodes.flatMapToPair(new NNCTPHBinFunction(stages, buckets));
    }
}

/**
 *
 * @author tibo
 */
class NNCTPHBinFunction
        implements PairFlatMapFunction<Node<String>, Integer, Node<String>> {

    private final int stages;
    private final int buckets;

    NNCTPHBinFunction(final int stages, final int buckets) {
        this.stages = stages;
        this.buckets = buckets;
    }

    @Override
    public Iterator<Tuple2<Integer, Node<String>>> call(final Node<String> n) {

        ESSum ess = new ESSum(stages, buckets, 1);

        ArrayList<Tuple2<Integer, Node<String>>> r = new ArrayList<>();
        int[] hash = ess.HashString(n.value);
        for (int v : hash) {
            r.add(new Tuple2<>(v, n));
        }

        return r.iterator();
    }
}
