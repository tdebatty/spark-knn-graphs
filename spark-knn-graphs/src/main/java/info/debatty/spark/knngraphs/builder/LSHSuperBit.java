package info.debatty.spark.knngraphs.builder;

import info.debatty.java.graphs.Node;
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
abstract class LSHSuperBit<T> extends AbstractPartitioningBuilder<T> {

    info.debatty.java.lsh.LSHSuperBit lsh;
    protected int dim;

    public void setDim(int dim) {
        this.dim = dim;
    }

    @Override
    protected JavaPairRDD<Integer, Node<T>> _binNodes(JavaRDD<Node<T>> nodes)  throws Exception {
        lsh = new info.debatty.java.lsh.LSHSuperBit(stages, buckets, this.dim);


        return nodes.flatMapToPair(
                new PairFlatMapFunction<Node<T>, Integer, Node<T>>() {

            public Iterator<Tuple2<Integer, Node<T>>> call(Node<T> n) throws Exception {
                ArrayList<Tuple2<Integer, Node<T>>> r = new ArrayList<Tuple2<Integer, Node<T>>>();
                int[] hash = hash(n.value);
                for (int v : hash) {
                    r.add(new Tuple2<Integer, Node<T>>(v, n));
                }

                return r.iterator();
            }
        });
    }

    abstract int[] hash(T node_value);
}
