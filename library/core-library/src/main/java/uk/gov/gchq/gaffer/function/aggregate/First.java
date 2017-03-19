/*
 * Copyright 2016-2017 Crown Copyright
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
package uk.gov.gchq.gaffer.function.aggregate;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.function.SimpleAggregateFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;
import uk.gov.gchq.gaffer.function.annotation.Outputs;

/**
 * An <code>First</code> is a {@link SimpleAggregateFunction} that assumes the
 * value will never change and just returns the first non null value it gets.
 */
@Inputs(Object.class)
@Outputs(Object.class)
public class First extends SimpleAggregateFunction<Object> {
    private Object aggregate;

    @Override
    public void init() {
        aggregate = null;
    }

    @Override
    protected Object _state() {
        return aggregate;
    }

    @Override
    protected void _aggregate(final Object input) {
        if (null != input && null == aggregate) {
            aggregate = input;
        }
    }

    public First statelessClone() {
        return new First();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final First that = (First) o;

        return new EqualsBuilder()
                .append(inputs, that.inputs)
                .append(outputs, that.outputs)
                .append(aggregate, that.aggregate)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(inputs)
                .append(outputs)
                .append(aggregate)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("inputs", inputs)
                .append("outputs", outputs)
                .append("aggregate", aggregate)
                .toString();
    }
}
