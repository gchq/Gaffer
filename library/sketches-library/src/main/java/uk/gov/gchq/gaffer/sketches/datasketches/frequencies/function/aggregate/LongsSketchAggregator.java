/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.sketches.datasketches.frequencies.function.aggregate;

import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.frequencies.LongsSketch;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.function.SimpleAggregateFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;
import uk.gov.gchq.gaffer.function.annotation.Outputs;

/**
 * A <code>LongsSketchAggregator</code> is a {@link SimpleAggregateFunction} that takes in
 * {@link LongsSketch}s and merges them together using {@link LongsSketch#merge(LongsSketch)}.
 */
@Inputs(LongsSketch.class)
@Outputs(LongsSketch.class)
public class LongsSketchAggregator extends SimpleAggregateFunction<LongsSketch> {
    private LongsSketch sketch;

    @Override
    public void init() {
    }


    @Override
    protected void _aggregate(final LongsSketch input) {
        if (input != null) {
            if (sketch == null) {
                // It would be better to create a new LongsSketch using something like
                // sketch = new LongsSketch(input.getMaximumMapCapacity());
                // but this doesn't work as getMaximumMapCapacity() doesn't return the right parameter
                // to pass to the constructor. We don't want to just set sketch = input as we want
                // to clone input. Instead we have to pay the expense of creating a new one
                // by serialising it and deserialising it again.
                sketch = LongsSketch.getInstance(new NativeMemory(input.toByteArray()));
            } else {
                sketch.merge(input);
            }
        }
    }

    @Override
    protected LongsSketch _state() {
        return sketch;
    }

    @Override
    public LongsSketchAggregator statelessClone() {
        final LongsSketchAggregator clone = new LongsSketchAggregator();
        clone.init();
        return clone;
    }

    /**
     * As a {@link LongsSketch} does not have an <code>equals()</code> method, the serialised form of the extracted
     * {@link LongsSketch} is used.
     *
     * @param o the object to test
     * @return true if o equals this object
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final LongsSketchAggregator that = (LongsSketchAggregator) o;
        final byte[] serialisedSketch = sketch == null ? null : sketch.toByteArray();
        final byte[] thatSerialisedSketch = that.sketch == null ? null : that.sketch.toByteArray();

        return new EqualsBuilder()
                .append(inputs, that.inputs)
                .append(outputs, that.outputs)
                .append(serialisedSketch, thatSerialisedSketch)
                .isEquals();
    }

    @Override
    public int hashCode() {
        final byte[] serialisedSketch = sketch == null ? null : sketch.toByteArray();

        if (serialisedSketch != null) {
            return new HashCodeBuilder(17, 37)
                    .append(inputs)
                    .append(outputs)
                    .append(serialisedSketch)
                    .toHashCode();
        } else {
            return new HashCodeBuilder(17, 37)
                    .append(inputs)
                    .append(outputs)
                    .toHashCode();
        }
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
