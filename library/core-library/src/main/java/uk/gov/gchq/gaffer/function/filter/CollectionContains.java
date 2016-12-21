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
package uk.gov.gchq.gaffer.function.filter;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.function.SimpleFilterFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;
import java.util.Collection;

/**
 * An <code>CollectionContains</code> is a {@link uk.gov.gchq.gaffer.function.SimpleFilterFunction}
 * that checks whether a {@link java.util.Collection} contains a provided value.
 */
@Inputs(Collection.class)
public class CollectionContains extends SimpleFilterFunction<Collection<?>> {
    private Object value;

    public CollectionContains() {
        // Required for serialisation
    }

    public CollectionContains(final Object value) {
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(final Object value) {
        this.value = value;
    }

    public CollectionContains statelessClone() {
        return new CollectionContains(value);
    }

    @Override
    protected boolean isValid(final Collection<?> input) {
        return input.contains(value);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final CollectionContains that = (CollectionContains) o;

        return new EqualsBuilder()
                .append(inputs, that.inputs)
                .append(value, that.value)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(inputs)
                .append(value)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("inputs", inputs)
                .append("value", value)
                .toString();
    }
}
