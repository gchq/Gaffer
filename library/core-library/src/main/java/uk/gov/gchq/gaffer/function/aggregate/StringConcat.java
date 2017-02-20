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
package uk.gov.gchq.gaffer.function.aggregate;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.function.SimpleAggregateFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;
import uk.gov.gchq.gaffer.function.annotation.Outputs;

/**
 * An <code>StringConcat</code> is a {@link SimpleAggregateFunction} that takes in
 * {@link String}s and concatenates them together. The default separator is a comma, you can set a custom
 * separator using setSeparator(String).
 */
@Inputs(String.class)
@Outputs(String.class)
public class StringConcat extends SimpleAggregateFunction<String> {
    private static final String DEFAULT_SEPARATOR = ",";
    private String aggregate;
    private String separator = DEFAULT_SEPARATOR;

    @Override
    public void init() {
        aggregate = null;
    }

    @Override
    protected String _state() {
        return aggregate;
    }

    @Override
    protected void _aggregate(final String input) {
        if (null != input) {
            if (null == aggregate) {
                aggregate = input;
            } else {
                aggregate = aggregate + separator + input;
            }
        }
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(final String separator) {
        this.separator = separator;
    }

    public StringConcat statelessClone() {
        StringConcat concat = new StringConcat();
        concat.setSeparator(getSeparator());
        return concat;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final StringConcat that = (StringConcat) o;

        return new EqualsBuilder()
                .append(inputs, that.inputs)
                .append(outputs, that.outputs)
                .append(aggregate, that.aggregate)
                .append(separator, that.separator)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(inputs)
                .append(outputs)
                .append(aggregate)
                .append(separator)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("inputs", inputs)
                .append("outputs", outputs)
                .append("aggregate", aggregate)
                .append("separator", separator)
                .toString();
    }
}
