package info.debatty.spark.knngraphs.builder;

/**
 *
 * @author tibo
 * @param <T>
 */
public abstract class AbstractPartitioningBuilder<T> extends AbstractBuilder<T> {
    protected int stages = 3;
    protected int buckets = 10;
    
    public void setStages(int stages) {
        this.stages = stages;
    }
    
    public void setBuckets(int buckets) {
        this.buckets = buckets;
    }
}
