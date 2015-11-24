package info.debatty.spark.knngraphs.builder;


import info.debatty.java.graphs.Neighbor;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Random;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Implementation of NN-Descent k-nn graph building algorithm.
 * Based on the paper "Efficient K-Nearest Neighbor Graph Construction for Generic Similarity Measures"
 * by Dong et al.
 * http://www.cs.princeton.edu/cass/papers/www11.pdf
 * 
 * NN-Descent works by iteratively exploring the neighbors of neighbors...
 * 
 * @author Thibault Debatty
 * @param <T> The class of nodes value
 */
public class NNDescent<T> extends DistributedGraphBuilder<T> implements Serializable {
    
    private int max_iterations = 10;
    
    /**
     * Set the maximum number of iterations.
     * Default value is 10
     * @param max_iterations
     * @return 
     */
    public NNDescent setMaxIterations(int max_iterations) {
        if (max_iterations <= 0) {
            throw new InvalidParameterException("max_iterations must be positive!");
        }
        
        this.max_iterations = max_iterations;
        return this;
    }

    /**
     *
     * @param nodes
     * @return
     */
    protected JavaPairRDD<Node<T>, NeighborList> _computeGraph(JavaRDD<Node<T>> nodes) {
        
        // Randomize: associate each node to 10 buckets out of 20
        JavaPairRDD<Integer, Node<T>> randomized = nodes.flatMapToPair(
                new PairFlatMapFunction<Node<T>, Integer, Node<T>>() {
            
            Random rand = new Random();
            
            public Iterable<Tuple2<Integer, Node<T>>> call(Node<T> n) throws Exception {
                ArrayList<Tuple2<Integer, Node<T>>> r = new ArrayList<Tuple2<Integer, Node<T>>>();
                for (int i = 0; i < 10; i++) {
                    r.add(new Tuple2<Integer, Node<T>>(rand.nextInt(20), n));
                }
                
                return r;
            }
        });
        
        // Inside bucket, associate
        JavaPairRDD<Node<T>, NeighborList> random_nl = randomized.groupByKey().flatMapToPair(
                new PairFlatMapFunction<Tuple2<Integer, Iterable<Node<T>>>, Node<T>, NeighborList>() {
                    
            Random rand = new Random();
            public Iterable<Tuple2<Node<T>, NeighborList>> call(
                    Tuple2<Integer, Iterable<Node<T>>> tuple) throws Exception {

                // Read all tuples in bucket
                ArrayList<Node<T>> nodes = new ArrayList<Node<T>>();
                for (Node<T> n : tuple._2) {
                    nodes.add(n);
                }

                ArrayList<Tuple2<Node<T>, NeighborList>> r = new ArrayList<Tuple2<Node<T>, NeighborList>>();
                for (Node<T> n : nodes) {
                    NeighborList nnl = new NeighborList(k);
                    for (int i = 0; i < k; i++) {
                        nnl.add(new Neighbor(
                                nodes.get(rand.nextInt(nodes.size())),
                                Double.MAX_VALUE));
                    }

                    r.add(new Tuple2<Node<T>, NeighborList>(n, nnl));
                }

                return r;
            }
        });
        
        // Merge
        JavaPairRDD<Node<T>, NeighborList> graph = random_nl.reduceByKey(
                
                new Function2<NeighborList, NeighborList, NeighborList>() {
            
            public NeighborList call(NeighborList nl1, NeighborList nl2) throws Exception {
                NeighborList nnl = new NeighborList(k);
                nnl.addAll(nl1);
                nnl.addAll(nl2);
                return nnl;
            }
        });
        
        for (int iteration = 0; iteration < max_iterations; iteration++) {
            
            // Reverse
            JavaPairRDD<Node<T>, Node<T>> exploded_graph = graph.flatMapToPair(
                    new PairFlatMapFunction<Tuple2<Node<T>, NeighborList>, Node<T>, Node<T>>() {
                
                public Iterable<Tuple2<Node<T>, Node<T>>> call(Tuple2<Node<T>, NeighborList> tuple) throws Exception {
                    
                    ArrayList<Tuple2<Node<T>, Node<T>>> r = new ArrayList<Tuple2<Node<T>, Node<T>>>();
                    
                    for (Neighbor neighbor : tuple._2()) {

                        r.add(new Tuple2<Node<T>, Node<T>>(tuple._1(), neighbor.node));
                        r.add(new Tuple2<Node<T>, Node<T>>(neighbor.node, tuple._1()));
                    }
                    return r;
                    
                }
            });
            
            
            // 
            graph = exploded_graph.groupByKey().flatMapToPair(
                    new PairFlatMapFunction<Tuple2<Node<T>, Iterable<Node<T>>>, Node<T>, NeighborList>() {

                public Iterable<Tuple2<Node<T>, NeighborList>> call(Tuple2<Node<T>, Iterable<Node<T>>> tuple) throws Exception {
                    
                    // Fetch all nodes
                    ArrayList<Node<T>> nodes = new ArrayList<Node<T>>();
                    nodes.add(tuple._1);
                    
                    for (Node<T> n : tuple._2) {
                        nodes.add(n);
                    }
                    
                    // 
                    ArrayList<Tuple2<Node<T>, NeighborList>> r = new ArrayList<Tuple2<Node<T>, NeighborList>>(nodes.size());
                    
                    for (Node<T> n : nodes) {
                        NeighborList nl = new NeighborList(k);
                        
                        for (Node<T> other : nodes) {
                            if (other.equals(n)) {
                                continue;
                            }
                            
                            nl.add(new Neighbor(
                                    other,
                                    similarity.similarity(n.value, other.value)));
                        }
                        
                        r.add(new Tuple2<Node<T>, NeighborList>(n, nl));
                    }
                    
                    return r;
                    
                }
            });
            
            // Filter
            graph = graph.groupByKey().mapToPair(new PairFunction<Tuple2<Node<T>, Iterable<NeighborList>>, Node<T>, NeighborList>() {

                public Tuple2<Node<T>, NeighborList> call(Tuple2<Node<T>, Iterable<NeighborList>> tuple) throws Exception {
                    NeighborList nl = new NeighborList(k);
                    
                    for (NeighborList other : tuple._2()) {
                        nl.addAll(other);
                    }
                    
                    return new Tuple2<Node<T>, NeighborList>(tuple._1, nl);
                }
            });
        }
        
        return graph;
    }
}
