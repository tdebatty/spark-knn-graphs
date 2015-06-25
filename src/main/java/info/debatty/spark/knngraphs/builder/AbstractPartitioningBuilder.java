package info.debatty.spark.knngraphs.builder;

import info.debatty.java.graphs.build.GraphBuilder;

/**
 *
 * @author tibo
 * @param <T>
 */
public abstract class AbstractPartitioningBuilder<T> extends AbstractBuilder<T> {
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
}
