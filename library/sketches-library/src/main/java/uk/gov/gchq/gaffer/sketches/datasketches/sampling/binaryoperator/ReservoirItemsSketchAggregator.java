/*
 * Copyright 2017-2023 Crown Copyright
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

import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.datasketches.sampling.ReservoirItemsUnion;

import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

/**
 * A {@code ReservoirItemsSketchAggregator} is a {@link java.util.function.BinaryOperator} that aggregates
 * {@link ReservoirItemsSketch}s using a {@link ReservoirItemsUnion}.
 */
@Since("1.0.0")
@Summary("Aggregates ReservoirItemsSketches")
public class ReservoirItemsSketchAggregator<T> extends KorypheBinaryOperator<ReservoirItemsSketch<T>> {

    @Override
    protected ReservoirItemsSketch<T> _apply(final ReservoirItemsSketch<T> a, final ReservoirItemsSketch<T> b) {
        final ReservoirItemsUnion<T> union = ReservoirItemsUnion.newInstance(a.getK());
        union.update(a);
        union.update(b);
        return union.getResult();
    }
}
