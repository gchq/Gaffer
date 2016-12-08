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
package gaffer.function.simple.aggregate;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import gaffer.function.SimpleAggregateFunction;
import gaffer.function.annotation.Inputs;
import gaffer.function.annotation.Outputs;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * An <code>HyperLogLogPlusAggregator</code> is a {@link SimpleAggregateFunction} that takes in
 * {@link com.clearspring.analytics.stream.cardinality.HyperLogLogPlus}s and merges the sketches together.
 */
@Inputs(HyperLogLogPlus.class)
@Outputs(HyperLogLogPlus.class)
public class HyperLogLogPlusAggregator extends SimpleAggregateFunction<HyperLogLogPlus> {
    private HyperLogLogPlus sketch;

    @Override
    public void init() {
        sketch = null;
    }

    @Override
    protected void _aggregate(final HyperLogLogPlus input) {
        if (input != null) {
            if (null == sketch) {
                sketch = new HyperLogLogPlus(5, 5);
            }
            try {
                sketch.addAll(input);
            } catch (CardinalityMergeException exception) {
                throw new RuntimeException("An Exception occurred when trying to aggregate the HyperLogLogPlus objects", exception);
            }
        }
    }

    @Override
    protected HyperLogLogPlus _state() {
        return sketch;
    }

    @Override
    public HyperLogLogPlusAggregator statelessClone() {
        HyperLogLogPlusAggregator clone = new HyperLogLogPlusAggregator();
        clone.init();
        return clone;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final HyperLogLogPlusAggregator that = (HyperLogLogPlusAggregator) o;

        return new EqualsBuilder()
                .append(inputs, that.inputs)
                .append(outputs, that.outputs)
                .append(sketch, that.sketch)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(inputs)
                .append(outputs)
                .append(sketch)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("inputs", inputs)
                .append("outputs", outputs)
                .append("sketch", sketch)
                .toString();
    }
}
