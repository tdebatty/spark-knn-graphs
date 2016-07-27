package info.devatty.spark.knngraphs.eval;

import info.debatty.java.graphs.SimilarityInterface;


/**
 *
 * @author Thibault Debatty
 */
public class L2Similarity implements SimilarityInterface<Double[]> {

    /**
     * 
     * @param value1
     * @param value2
     * @return
     */
    public final double similarity(
            final Double[] value1, final Double[] value2) {

        double agg = 0;
        for (int i = 0; i < value1.length; i++) {
            agg += (value1[i] - value2[i]) * (value1[i] - value2[i]);
        }

        return 1.0 / (1 + Math.sqrt(agg));
    }
}
