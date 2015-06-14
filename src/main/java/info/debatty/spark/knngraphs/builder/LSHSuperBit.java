package info.debatty.spark.knngraphs.builder;

import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.build.Brute;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 *
 * @author tibo
 */
abstract class LSHSuperBit<T> extends AbstractPartitioningBuilder<T> {
    
    info.debatty.java.lsh.LSHSuperBit lsh;
    protected int dim;
    
    public void setDim(int dim) {
        this.dim = dim;
    }
    
    @Override
    protected JavaPairRDD<Node<T>, NeighborList> _computeGraph(JavaRDD<Node<T>> nodes) {
        
        
        lsh = new info.debatty.java.lsh.LSHSuperBit(stages, buckets, dim);
        //lsh.hash(vector);
        
        JavaPairRDD<Integer, Node<T>> bucketsofnodes = nodes.flatMapToPair(
                new PairFlatMapFunction<Node<T>, Integer, Node<T>>() {
            
            public Iterable<Tuple2<Integer, Node<T>>> call(Node<T> n) throws Exception {
                ArrayList<Tuple2<Integer, Node<T>>> r = new ArrayList<Tuple2<Integer, Node<T>>>();
                int[] hash = hash(n.value);
                for (int v : hash) {
                    r.add(new Tuple2<Integer, Node<T>>(v, n));
                }
                
                return r;
            }
        });
        
        JavaPairRDD<Node<T>, NeighborList> graph = bucketsofnodes.groupByKey().flatMapToPair(
                new  PairFlatMapFunction<Tuple2<Integer, Iterable<Node<T>>>, Node<T>, NeighborList>() {
            
            public Iterable<Tuple2<Node<T>, NeighborList>> call(Tuple2<Integer, Iterable<Node<T>>> tuple) throws Exception {
                ArrayList<Node<T>> nodes = new ArrayList<Node<T>>();
                for (Node<T> n : tuple._2) {
                    nodes.add(n);
                }
                
                Brute b = new Brute();
                b.setK(10);
                b.setSimilarity(similarity);
                HashMap graph = b.computeGraph(nodes);
                
                ArrayList<Tuple2<Node<T>, NeighborList>> r = new ArrayList<Tuple2<Node<T>, NeighborList>>();
                for (Object e : graph.entrySet()) {
                    Entry<Node, NeighborList> entry = (Entry<Node, NeighborList>) e;
                    r.add(new Tuple2<Node<T>, NeighborList>(entry.getKey(), entry.getValue()));
                }
                
                return r;
            }
        });
        
        graph = graph.groupByKey().mapToPair(
                new PairFunction<Tuple2<Node<T>, Iterable<NeighborList>>, Node<T>, NeighborList>() {

            public Tuple2<Node<T>, NeighborList> call(Tuple2<Node<T>, Iterable<NeighborList>> tuple) throws Exception {
                NeighborList nl = new NeighborList(k);
                for (NeighborList n : tuple._2) {
                    nl.addAll(n);
                }
                return new Tuple2<Node<T>, NeighborList>(tuple._1, nl);
            }
        });
        
        return graph;
    }

    abstract int[] hash(T node_value);
}
