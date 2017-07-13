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
package info.debatty.spark.knngraphs;

import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;

/**
 * Used by sequential k-medoids to convert the similarity between T (provided
 * to the partitioner) to a similarity between Node<T> (required by sequential
 * k-medoids).
 * @author Thibault Debatty
 * @param <T>
 */
class SimilarityAdapter<T> implements SimilarityInterface<Node<T>> {
    private final SimilarityInterface<T> internal_similarity;

    SimilarityAdapter(final SimilarityInterface<T> similarity) {
        this.internal_similarity = similarity;
    }

    /**
     * Compute similarity between nodes.
     * @param node1
     * @param node2
     * @return
     */
    public double similarity(final Node<T> node1, final Node<T> node2) {
        return internal_similarity.similarity(node1.value, node2.value);
    }
}
