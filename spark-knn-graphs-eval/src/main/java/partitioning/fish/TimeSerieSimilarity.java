/*
 * The MIT License
 *
 * Copyright 2017 tibo.
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
package partitioning.fish;

import info.debatty.java.datasets.fish.DTWSimilarity;
import info.debatty.java.datasets.fish.TimeSerie;
import info.debatty.java.graphs.SimilarityInterface;

/**
 *
 * @author tibo
 */
public class TimeSerieSimilarity implements SimilarityInterface<TimeSerie> {

    private final DTWSimilarity dtw = new DTWSimilarity();

    /**
     *
     * @param arg0
     * @param arg1
     * @return
     */
    public final double similarity(final TimeSerie arg0, final TimeSerie arg1) {
        return dtw.similarity(arg0.values, arg1.values);
    }
}
