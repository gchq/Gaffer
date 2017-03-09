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
package uk.gov.gchq.gaffer.sketches.datasketches.quantiles.function.aggregate;

import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.quantiles.ItemsUnion;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.function.SimpleAggregateFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;
import uk.gov.gchq.gaffer.function.annotation.Outputs;

/**
 * A <code>StringsUnionAggregator</code> is a {@link SimpleAggregateFunction} that aggregates {@link ItemsUnion}s
 * of {@link String}s. It does this by extracting a {@link com.yahoo.sketches.quantiles.ItemsSketch} from each
 * {@link ItemsUnion} and merges that using {@link ItemsUnion#update(com.yahoo.sketches.quantiles.ItemsSketch)}.
 */
@Inputs(ItemsUnion.class)
@Outputs(ItemsUnion.class)
public class StringsUnionAggregator extends SimpleAggregateFunction<ItemsUnion<String>> {
    private static final ArrayOfStringsSerDe SERIALISER = new ArrayOfStringsSerDe();
    private ItemsUnion<String> union;

    @Override
    public void init() {
    }

    @Override
    protected void _aggregate(final ItemsUnion<String> input) {
        if (input != null) {
            if (union == null) {
                union = ItemsUnion.getInstance(input.getResult());
            } else {
                union.update(input.getResult());
            }
        }
    }

    @Override
    protected ItemsUnion<String> _state() {
        return union;
    }

    @Override
    public StringsUnionAggregator statelessClone() {
        final StringsUnionAggregator clone = new StringsUnionAggregator();
        clone.init();
        return clone;
    }

    /**
     * As an {@link ItemsUnion} does not have an <code>equals()</code> method, the serialised form of the extracted
     * {@link com.yahoo.sketches.quantiles.ItemsSketch} is used.
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

        final StringsUnionAggregator that = (StringsUnionAggregator) o;
        final byte[] serialisedUnion = union == null ? null : union.getResult().toByteArray(SERIALISER);
        final byte[] thatSerialisedUnion = that.union == null ? null : that.union.getResult().toByteArray(SERIALISER);

        return new EqualsBuilder()
                .append(inputs, that.inputs)
                .append(outputs, that.outputs)
                .append(serialisedUnion, thatSerialisedUnion)
                .isEquals();
    }

    @Override
    public int hashCode() {
        final byte[] serialisedSketch = union == null ? null : union.getResult().toByteArray(SERIALISER);

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
