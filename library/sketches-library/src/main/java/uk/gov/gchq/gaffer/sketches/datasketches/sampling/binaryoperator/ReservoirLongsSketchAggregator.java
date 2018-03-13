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

import com.yahoo.sketches.sampling.ReservoirLongsSketch;
import com.yahoo.sketches.sampling.ReservoirLongsUnion;

import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

/**
 * A {@code ReservoirLongsSketchAggregator} is a {@link java.util.function.BinaryOperator} that aggregates
 * {@link ReservoirLongsSketch}s using a {@link ReservoirLongsUnion}.
 */
@Since("1.0.0")
public class ReservoirLongsSketchAggregator extends KorypheBinaryOperator<ReservoirLongsSketch> {

    @Override
    protected ReservoirLongsSketch _apply(final ReservoirLongsSketch a, final ReservoirLongsSketch b) {
        final ReservoirLongsUnion union = ReservoirLongsUnion.newInstance(a.getK());
        union.update(a);
        union.update(b);
        return union.getResult();
    }
}
