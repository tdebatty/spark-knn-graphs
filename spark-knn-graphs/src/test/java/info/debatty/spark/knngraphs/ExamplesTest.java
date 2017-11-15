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

import java.util.Set;
import junit.framework.TestCase;
import org.reflections.Reflections;

/**
 *
 * @author tibo
 */
public class ExamplesTest extends TestCase {

    /**
     * Try to run all examples.
     *
     */
    public final void testExamples() throws Exception {
        Reflections reflections = new Reflections(
                "info.debatty.spark.knngraphs.example");
        Set<Class<? extends Runnable>> examples =
                reflections.getSubTypesOf(Runnable.class);

        boolean fail = false;

        for (Class<? extends Runnable> example_class : examples) {
            try {
                Runnable instance = example_class.newInstance();
                instance.run();
            } catch (Exception ex) {
                System.out.println("Example failed: "
                        + example_class.getCanonicalName());
                fail = true;
            }
        }

        if (fail) {
            throw new Exception("One or more example(s) failed!");
        }
    }
}
