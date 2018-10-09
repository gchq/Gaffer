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
package uk.gov.gchq.gaffer.sketches.datasketches.theta.binaryoperator;

import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Union;

import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

/**
 * A {@code SketchAggregator} is a {@link java.util.function.BinaryOperator} that aggregates {@link Sketch}s
 * using a {@link Union}.
 */
@Since("1.0.0")
@Summary("Aggregates Sketches using a Union")
public class SketchAggregator extends KorypheBinaryOperator<Sketch> {

    @Override
    protected Sketch _apply(final Sketch a, final Sketch b) {
        final Union union = Sketches.setOperationBuilder().buildUnion();
        union.update(a);
        union.update(b);
        return union.getResult();
    }
}
