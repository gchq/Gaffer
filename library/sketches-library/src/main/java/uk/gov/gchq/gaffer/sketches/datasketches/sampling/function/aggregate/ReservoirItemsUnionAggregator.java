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
package uk.gov.gchq.gaffer.sketches.datasketches.sampling.function.aggregate;

import com.yahoo.sketches.sampling.ReservoirItemsUnion;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.function.SimpleAggregateFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;
import uk.gov.gchq.gaffer.function.annotation.Outputs;

/**
 * A <code>ReservoirItemsUnionAggregator</code> is a {@link SimpleAggregateFunction} that aggregates
 * {@link ReservoirItemsUnion}s. It does this by extracting a {@link com.yahoo.sketches.sampling.ReservoirItemsSketch}
 * from each {@link ReservoirItemsUnion} and merges that using
 * {@link ReservoirItemsUnion#update(com.yahoo.sketches.sampling.ReservoirItemsSketch)}.
 * @param <T> The type of object in the reservoir.
 */
@Inputs(ReservoirItemsUnion.class)
@Outputs(ReservoirItemsUnion.class)
public class ReservoirItemsUnionAggregator<T> extends SimpleAggregateFunction<ReservoirItemsUnion<T>> {
    private ReservoirItemsUnion<T> union;

    @Override
    public void init() {
    }

    @Override
    protected void _aggregate(final ReservoirItemsUnion<T> input) {
        if (input != null) {
            if (union == null) {
                union = ReservoirItemsUnion.getInstance(input.getMaxK());
            }
            union.update(input.getResult());
        }
    }

    @Override
    protected ReservoirItemsUnion<T> _state() {
        return union;
    }

    @Override
    public ReservoirItemsUnionAggregator statelessClone() {
        final ReservoirItemsUnionAggregator<T> clone = new ReservoirItemsUnionAggregator<>();
        clone.init();
        return clone;
    }

    // NB: No equals() method is provided for this class as neither ReservoirItemsUnion nor ReservoirItemsSketch
    // have an equals() method, and it is not possible to compare the serialised forms as serialisation requires
    // a serialiser for T.

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("inputs", inputs)
                .append("outputs", outputs)
                .append("union", union)
                .toString();
    }
}
