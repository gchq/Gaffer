/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.function;

/**
 * A <code>FilterFunction</code> is a {@link uk.gov.gchq.gaffer.function.ConsumerFunction} that tests input records against some
 * criteria, returning a <code>boolean</code> result to indicate whether the input passes or fails the test. The result
 * of any filter can be reversed by setting the <code>not</code> option.
 * <p>
 * For example:<br>
 * <code>FilterFunction isA = new IsA(Integer.class);</code><br>
 * <code>boolean result = isA.isValid({1}); //result = true</code><br>
 * <code>result = isA.isValid({"a"}); //result = false</code><br>
 * <code>isA.setNot(true); // flip results</code>
 * <code>result = isA.isValid({1}); //result = false</code><br>
 * <code>result = isA.isValid({"a"}) // result = true</code>
 */
public abstract class FilterFunction extends ConsumerFunction {
    /**
     * Executes this <code>FilterFunction</code> with an input record. Input records should match the types reported by
     * <code>getInputClasses()</code>.
     *
     * @param input Input record to test.
     * @return true if input record passes the test.
     */
    public abstract boolean isValid(final Object[] input);

    @Override
    public abstract FilterFunction statelessClone();
}
