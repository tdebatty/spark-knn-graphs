package info.debatty.spark.knngraphs;

import java.util.ArrayList;

/**
 * Helper class to perform the sum of a large number of values.
 * A double value in java only has 15 significant digits. Hence, when performing
 * the sum of a large number of double values, the precision of the result
 * may be highly altered. For example, if performing the sum of 1E6 values,
 * the result only has 9 significant digits. This helper class performs the
 * addition using groups of 1E5 values. The result has 10 significant digits,
 * for sums of up to 1E10 values.
 * @author Thibault Debatty
 */
public class SumAggregator {

    private static final int CAPACITY = 100000;

    private double value = 0;
    private ArrayList<Double> stack = new ArrayList<Double>(CAPACITY);

    /**
     * Add a value to the aggregator.
     * @param value
     */
    public final void add(final double value) {
        stack.add(value);

        if (stack.size() == CAPACITY) {
            double stack_value = 0;
            for (Double d : stack) {
                stack_value += d;
            }
            this.value += stack_value;
            stack = new ArrayList<Double>(CAPACITY);
        }
    }

    /**
     * Get the current value of the aggregator.
     * @return
     */
    public final double value() {
        double stack_value = 0;
        for (Double d : stack) {
            stack_value += d;
        }
        return value + stack_value;
    }
}
