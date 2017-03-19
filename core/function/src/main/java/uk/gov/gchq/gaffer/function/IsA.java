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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.function.annotation.Inputs;

/**
 * An <code>IsA</code> {@link uk.gov.gchq.gaffer.function.FilterFunction} tests whether an input {@link java.lang.Object} is an
 * instance of some control {@link java.lang.Class}.
 */
@Inputs(Object.class)
public class IsA extends SimpleFilterFunction<Object> {
    private Class<?> type;

    /**
     * Default constructor - used for serialisation.
     */
    public IsA() {
    }

    /**
     * Create an <code>IsA</code> filter that tests for instances of a given control {@link java.lang.Class}.
     *
     * @param type Control class.
     */
    public IsA(final Class<?> type) {
        this.type = type;
    }

    /**
     * Create an <code>IsA</code> filter that tests for instances of a given control class name.
     *
     * @param type Name of the control class.
     */
    public IsA(final String type) {
        setType(type);
    }

    /**
     * @param type Name of the control class.
     */
    public void setType(final String type) {
        try {
            this.type = Class.forName(type);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not load class for given type: " + type, e);
        }
    }

    /**
     * @return Name of the control class.
     */
    public String getType() {
        return null != type ? type.getName() : null;
    }

    /**
     * Create a new <code>IsA</code> filter with the same control class as this one.
     *
     * @return New <code>IsA</code> filter.
     */
    public IsA statelessClone() {
        return new IsA(type);
    }

    /**
     * Tests whether the argument supplied to this method is an instance of the control class.
     *
     * @param input {@link java.lang.Object} to test.
     * @return true iff there is a single, non-null input object that can be cast to the control class, otherwise false.
     */
    @Override
    public boolean isValid(final Object input) {
        return null == input || type.isAssignableFrom(input.getClass());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final IsA isA = (IsA) o;

        return new EqualsBuilder()
                .appendSuper(super.equals(o))
                .append(inputs, isA.inputs)
                .append(type, isA.type)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .appendSuper(super.hashCode())
                .append(inputs)
                .append(type)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("inputs", inputs)
                .append("type", type)
                .toString();
    }
}
