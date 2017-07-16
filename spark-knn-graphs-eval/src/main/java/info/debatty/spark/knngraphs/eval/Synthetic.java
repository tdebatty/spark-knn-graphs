/*
 * The MIT License
 *
 * Copyright 2016 Thibault Debatty.
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

package info.debatty.spark.knngraphs.eval;

import info.debatty.java.datasets.gaussian.Dataset;
import org.apache.commons.cli.ParseException;

public class Synthetic extends AbstractTest<Double[]> {

    private static final int DIMENSIONALITY = 20;
    private static final int CENTERS = 15;

    /**
     *
     * @param args
     * @throws ParseException if we cannot parse args
     * @throws Exception if the test fails to run
     */
    public static void main(final String[] args)
            throws ParseException, Exception  {

        Synthetic test = new Synthetic();
        test.parseArgs(args);

        test.setSimilarity(new L2Similarity());

        Dataset dataset = new Dataset.Builder(DIMENSIONALITY, CENTERS)
                .setOverlap(Dataset.Builder.Overlap.MEDIUM)
                .build();
        test.setDataSource(dataset.iterator());

        test.run();

    }
}