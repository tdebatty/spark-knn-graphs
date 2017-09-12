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

import info.debatty.java.graphs.Node;
import info.debatty.java.spamsum.ESSum;
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
    protected JavaPairRDD<Integer, Node<String>> _binNodes(JavaRDD<Node<String>> nodes) {
        return nodes.flatMapToPair(
                new PairFlatMapFunction<Node<String>, Integer, Node<String>>() {

            public Iterator<Tuple2<Integer, Node<String>>> call(Node<String> n) throws Exception {

                ESSum ess = new ESSum(stages, buckets, 1);

                ArrayList<Tuple2<Integer, Node<String>>> r = new ArrayList<Tuple2<Integer, Node<String>>>();
                int[] hash = ess.HashString(n.value);
                for (int v : hash) {
                    r.add(new Tuple2<Integer, Node<String>>(v, n));
                }

                return r.iterator();
            }
        });
    }

}
