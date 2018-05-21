/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.sketches.datasketches.sampling.binaryoperator;

import com.yahoo.sketches.sampling.ReservoirItemsUnion;

import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

/**
 * A {@code ReservoirItemsUnionAggregator} is a {@link java.util.function.BinaryOperator} that aggregates
 * {@link ReservoirItemsUnion}s. It does this by extracting a {@link com.yahoo.sketches.sampling.ReservoirItemsSketch}
 * from each {@link ReservoirItemsUnion} and merges that using
 * {@link ReservoirItemsUnion#update(com.yahoo.sketches.sampling.ReservoirItemsSketch)}.
 *
 * @param <T> The type of object in the reservoir.
 */
@Since("1.0.0")
@Summary("Aggregates ReservoirItemsUnions")
public class ReservoirItemsUnionAggregator<T> extends KorypheBinaryOperator<ReservoirItemsUnion<T>> {

    @Override
    public ReservoirItemsUnion<T> _apply(final ReservoirItemsUnion<T> a, final ReservoirItemsUnion<T> b) {
        a.update(b.getResult());
        return a;
    }
}
