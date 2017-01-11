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
package uk.gov.gchq.gaffer.sketches.datasketches.theta.function.aggregate;

import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Union;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.function.SimpleAggregateFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;
import uk.gov.gchq.gaffer.function.annotation.Outputs;

/**
 * A <code>UnionAggregator</code> is a {@link SimpleAggregateFunction} that aggregates {@link Union}s.
 * It does this by extracting a {@link com.yahoo.sketches.theta.CompactSketch} from each {@link Union}
 * and merges that using {@link Union#update(com.yahoo.sketches.theta.Sketch)}.
 */
@Inputs(Union.class)
@Outputs(Union.class)
public class UnionAggregator extends SimpleAggregateFunction<Union> {
    private Union union;

    @Override
    public void init() {
    }

    @Override
    protected void _aggregate(final Union input) {
        if (input != null) {
            if (union == null) {
                union = SetOperation.builder().buildUnion();
            }
            union.update(input.getResult());
        }
    }

    @Override
    protected Union _state() {
        return union;
    }

    @Override
    public UnionAggregator statelessClone() {
        final UnionAggregator clone = new UnionAggregator();
        clone.init();
        return clone;
    }

    /**
     * As a {@link Union} does not have an <code>equals()</code> method, the serialised form of the extracted
     * {@link com.yahoo.sketches.theta.CompactSketch} is used.
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

        final UnionAggregator that = (UnionAggregator) o;
        final byte[] serialisedUnion = union == null ? null : union.getResult().toByteArray();
        final byte[] thatSerialisedUnion = that.union == null ? null : that.union.getResult().toByteArray();

        return new EqualsBuilder()
                .append(inputs, that.inputs)
                .append(outputs, that.outputs)
                .append(serialisedUnion, thatSerialisedUnion)
                .isEquals();
    }

    @Override
    public int hashCode() {
        final byte[] serialisedSketch = union == null ? null : union.getResult().toByteArray();

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
                .append("union", union)
                .toString();
    }
}
