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

package uk.gov.gchq.koryphe.predicate;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import java.util.function.Predicate;

/**
 * An <code>IsA</code> {@link Predicate} tests whether an input {@link Object} is an
 * instance of a given control {@link Class}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
public class IsA extends KoryphePredicate<Object> {
    private Class<?> type;

    /**
     * Default constructor - used for serialisation.
     */
    public IsA() {
    }

    /**
     * Create an <code>IsA</code> validate that tests for instances of a given control {@link Class}.
     *
     * @param type Control class.
     */
    public IsA(final Class<?> type) {
        this.type = type;
    }

    /**
     * Create an <code>IsA</code> validate that tests for instances of a given control class name.
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
            throw new IllegalArgumentException("Could not load class for given type: " + type);
        }
    }

    /**
     * @return Name of the control class.
     */
    public String getType() {
        return null != type ? type.getName() : null;
    }

    /**
     * Tests whether the argument supplied is an instance of the control class.
     *
     * @param input {@link Object} to test.
     * @return true iff input is null or non-null and can be cast to the control class, otherwise false.
     */
    @Override
    public boolean test(final Object input) {
        return null == input || type.isAssignableFrom(input.getClass());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        final IsA isA = (IsA) o;
        return new EqualsBuilder()
                .append(type, isA.type)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .appendSuper(super.hashCode())
                .append(type)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("type", type)
                .toString();
    }
}
