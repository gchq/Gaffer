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

package gaffer.function;

/**
 * A <code>FilterFunction</code> is a {@link gaffer.function.ConsumerFunction} that tests input records against some
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
    private boolean not = false;

    /**
     * @return true if the result of this <code>FilterFunction</code> is reversed.
     */
    public boolean isNot() {
        return not;
    }

    /**
     * @param not if the result of this <code>FilterFunction</code> is reversed.
     */
    public void setNot(final boolean not) {
        this.not = not;
    }

    /**
     * Sets the not flag to true.
     *
     * @return this filter function.
     */
    public FilterFunction not() {
        setNot(true);
        return this;
    }


    /**
     * Executes this <code>FilterFunction</code> with an input record, reversing the result if
     * <code>isNot() == true</code>. Input records should match the types reported by <code>getInputClasses()</code>.
     *
     * @param input the input objects.
     * @return true if the input record is valid, otherwise false.
     */
    public boolean isValid(final Object[] input) {
        boolean result = _isValid(input);
        if (not) {
            return !result;
        } else {
            return result;
        }
    }

    @Override
    public abstract FilterFunction statelessClone();

    /**
     * Executes this <code>FilterFunction</code> with an input record. Input records should match the types reported by
     * <code>getInputClasses()</code>.
     *
     * @param input Input record to test.
     * @return true if input record passes the test.
     */
    protected abstract boolean _isValid(final Object[] input);
}
