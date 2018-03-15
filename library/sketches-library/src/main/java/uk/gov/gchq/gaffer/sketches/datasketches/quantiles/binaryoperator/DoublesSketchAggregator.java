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
package uk.gov.gchq.gaffer.sketches.datasketches.quantiles.binaryoperator;

import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesUnion;

import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

/**
 * A {@code DoublesSketchAggregator} is a {@link java.util.function.BinaryOperator} that aggregates
 * {@link DoublesSketch}s using a {@link DoublesUnion}.
 */
@Since("1.0.0")
public class DoublesSketchAggregator extends KorypheBinaryOperator<DoublesSketch> {

    @Override
    protected DoublesSketch _apply(final DoublesSketch a, final DoublesSketch b) {
        final DoublesUnion union = DoublesUnion.builder().setMaxK(a.getK()).build();
        union.update(a);
        union.update(b);
        return union.getResult();
    }
}
