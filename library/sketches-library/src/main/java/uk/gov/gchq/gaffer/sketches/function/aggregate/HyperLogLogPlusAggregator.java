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
package uk.gov.gchq.gaffer.sketches.function.aggregate;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.function.SimpleAggregateFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;
import uk.gov.gchq.gaffer.function.annotation.Outputs;
import java.io.IOException;

/**
 * An <code>HyperLogLogPlusAggregator</code> is a {@link SimpleAggregateFunction} that takes in
 * {@link HyperLogLogPlus}s and merges the sketches together.
 */
@Inputs(HyperLogLogPlus.class)
@Outputs(HyperLogLogPlus.class)
public class HyperLogLogPlusAggregator extends SimpleAggregateFunction<HyperLogLogPlus> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HyperLogLogPlusAggregator.class);
    private HyperLogLogPlus sketch;

    @Override
    public void init() {
        sketch = null;
    }

    @Override
    protected void _aggregate(final HyperLogLogPlus input) {
        if (input != null) {
            if (null == sketch) {
                try {
                    sketch = HyperLogLogPlus.Builder.build(input.getBytes());
                } catch (final IOException e) {
                    LOGGER.warn("Unable to clone a HyperLogLogPlus object", e);
                    sketch = input;
                }
            } else {
                try {
                    sketch.addAll(input);
                } catch (final CardinalityMergeException exception) {
                    throw new RuntimeException("An Exception occurred when trying to aggregate the HyperLogLogPlus objects", exception);
                }
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

        if (null == sketch) {
            return null == that.sketch;
        }

        if (null == that.sketch) {
            return false;
        }

        try {
            return new EqualsBuilder()
                    .append(inputs, that.inputs)
                    .append(outputs, that.outputs)
                    .append(sketch.getBytes(), that.sketch.getBytes())
                    .isEquals();
        } catch (final IOException e) {
            LOGGER.warn("Could not compare HyperLogLogPlus objects using their bytes", e);
            return false;
        }
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
