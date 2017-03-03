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
import java.util.Set;

/**
 * An <code>SetAggregator</code> is a {@link SimpleAggregateFunction} that
 * combines {@link Set}s.
 * <p>
 * NOTE - the set implementation must have a default constructor.
 * </p>
 *
 * @param <T> the type of objects the set contains.
 */
@Inputs(Set.class)
@Outputs(Set.class)
public class SetAggregator<T> extends SimpleAggregateFunction<Set<T>> {
    private Set<T> set;

    @Override
    protected void _aggregate(final Set<T> input) {
        if (null != input) {
            if (null == set) {
                try {
                    set = input.getClass().newInstance();
                } catch (InstantiationException | IllegalAccessException e) {
                    throw new IllegalArgumentException("Unable to create new instance of " + input.getClass().getName()
                            + ". This set aggregator can only be used on sets with a default constructor.", e);
                }
            }

            set.addAll(input);
        }
    }

    @Override
    public void init() {
        set = null;
    }

    @Override
    protected Set<T> _state() {
        return set;
    }

    @Override
    public SetAggregator statelessClone() {
        final SetAggregator aggregator = new SetAggregator();
        aggregator.init();
        return aggregator;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final SetAggregator<?> that = (SetAggregator<?>) o;

        return new EqualsBuilder()
                .append(inputs, that.inputs)
                .append(outputs, that.outputs)
                .append(set, that.set)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(inputs)
                .append(outputs)
                .append(set)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("inputs", inputs)
                .append("outputs", outputs)
                .append("set", set)
                .toString();
    }
}
