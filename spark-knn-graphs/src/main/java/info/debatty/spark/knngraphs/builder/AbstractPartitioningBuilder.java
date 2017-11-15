package info.debatty.spark.knngraphs.builder;

import info.debatty.java.graphs.Graph;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.SimilarityInterface;
import info.debatty.java.graphs.build.Brute;
import info.debatty.java.graphs.build.GraphBuilder;
import info.debatty.spark.knngraphs.Node;
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
public abstract class AbstractPartitioningBuilder<T>
        extends DistributedGraphBuilder<T> {

    protected int stages = 3;
    protected int buckets = 10;

    protected GraphBuilder<Node<T>> inner_graph_builder;

    public void setStages(final int stages) {
        this.stages = stages;
    }

    public void setBuckets(final int buckets) {
        this.buckets = buckets;
    }

    public void setInnerGraphBuilder(
            final GraphBuilder<Node<T>> inner_graph_builder) {
        this.inner_graph_builder = inner_graph_builder;
    }

    @Override
    protected JavaPairRDD<Node<T>, NeighborList> doComputeGraph(
            final JavaRDD<Node<T>> nodes)
            throws Exception {

        if (inner_graph_builder == null) {
            this.inner_graph_builder = new Brute();
        }

        inner_graph_builder.setK(k);
        inner_graph_builder.setSimilarity(
                new NodeSimilarityAdapter<>(similarity));

        // assign a bucket id to each node
        // each node is assigned to <stages> buckets (oversampling)
        JavaPairRDD<Integer, Node<T>> bucketsofnodes = bin(nodes);

        // compute the disconnected subgraphs
        JavaPairRDD<Node<T>, NeighborList> graph =
                bucketsofnodes.groupByKey().flatMapToPair(
                new  ComputeSubgraphsFunction(inner_graph_builder));


        graph = graph.groupByKey().mapToPair(
                new MergeSubgraphsFunction(k));

        return graph;
    }

    protected abstract JavaPairRDD<Integer, Node<T>> bin(JavaRDD<Node<T>> nodes)
            throws Exception;
}

class ComputeSubgraphsFunction<T>
        implements PairFlatMapFunction<
        Tuple2<Integer, Iterable<Node<T>>>, Node<T>, NeighborList> {

    private final GraphBuilder<Node<T>> inner_graph_builder;

    ComputeSubgraphsFunction(final GraphBuilder<Node<T>> graph_builder) {

        this.inner_graph_builder = graph_builder;
    }


    @Override
    public Iterator<Tuple2<Node<T>, NeighborList>> call(
            final Tuple2<Integer, Iterable<Node<T>>> tuple)
            throws Exception {
        ArrayList<Node<T>> nodes = new ArrayList<>();
        for (Node<T> n : tuple._2) {
            nodes.add(n);
        }

        Graph<Node<T>> graph = inner_graph_builder.computeGraph(nodes);

        ArrayList<Tuple2<Node<T>, NeighborList>> r = new ArrayList<>();
        for (Node<T> node : graph.getNodes()) {
            r.add(new Tuple2<>(node, graph.getNeighbors(node)));
        }
        return r.iterator();
    }
}

class MergeSubgraphsFunction<T>
        implements PairFunction<
        Tuple2<T, Iterable<NeighborList>>, T, NeighborList> {

    private final int k;

    MergeSubgraphsFunction(final int k) {
        this.k = k;
    }

    public Tuple2<T, NeighborList> call(
            final Tuple2<T, Iterable<NeighborList>> tuple) {
        NeighborList nl = new NeighborList(k);
        for (NeighborList n : tuple._2) {
            nl.addAll(n);
        }
        return new Tuple2<T, NeighborList>(tuple._1, nl);
    }
}