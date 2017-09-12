package info.debatty.spark.knngraphs.builder;

import info.debatty.java.graphs.Graph;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.build.Brute;
import info.debatty.java.graphs.build.GraphBuilder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 *
 * @author Thibault Debatty
 * @param <T>
 */
public abstract class AbstractPartitioningBuilder<T> extends DistributedGraphBuilder<T> {
    protected int stages = 3;
    protected int buckets = 10;

    protected GraphBuilder<T> inner_graph_builder;

    public void setStages(int stages) {
        this.stages = stages;
    }

    public void setBuckets(int buckets) {
        this.buckets = buckets;
    }

    public void setInnerGraphBuilder(GraphBuilder<T> inner_graph_builder) {
        this.inner_graph_builder = inner_graph_builder;
    }

    @Override
    protected JavaPairRDD<Node<T>, NeighborList> doComputeGraph(JavaRDD<Node<T>> nodes) throws Exception {

        JavaPairRDD<Integer, Node<T>> bucketsofnodes = _binNodes(nodes);

        JavaPairRDD<Node<T>, NeighborList> graph = bucketsofnodes.groupByKey().flatMapToPair(
                new  PairFlatMapFunction<Tuple2<Integer, Iterable<Node<T>>>, Node<T>, NeighborList>() {

            public Iterator<Tuple2<Node<T>, NeighborList>> call(Tuple2<Integer, Iterable<Node<T>>> tuple) throws Exception {
                ArrayList<Node<T>> nodes = new ArrayList<Node<T>>();
                for (Node<T> n : tuple._2) {
                    nodes.add(n);
                }

                if (inner_graph_builder == null) {
                    inner_graph_builder = new Brute();
                }
                inner_graph_builder.setK(k);
                inner_graph_builder.setSimilarity(similarity);
                Graph<T> graph = inner_graph_builder.computeGraph(nodes);

                ArrayList<Tuple2<Node<T>, NeighborList>> r = new ArrayList<Tuple2<Node<T>, NeighborList>>();
                for (Object e : graph.entrySet()) {
                    Map.Entry<Node, NeighborList> entry = (Map.Entry<Node, NeighborList>) e;
                    r.add(new Tuple2<Node<T>, NeighborList>(entry.getKey(), entry.getValue()));
                }

                return r.iterator();
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

    protected abstract JavaPairRDD<Integer, Node<T>> _binNodes(JavaRDD<Node<T>> nodes) throws Exception;
}
