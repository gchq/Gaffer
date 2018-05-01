/*
 * Copyright 2018 Crown Copyright
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

import com.yahoo.sketches.kll.KllFloatsSketch;

import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

/**
 * A {@code KllFloatsSketchAggregator} is a {@link KorypheBinaryOperator} that aggregates
 * {@link KllFloatsSketch}s.
 */
@Since("1.4.0")
public class KllFloatsSketchAggregator extends KorypheBinaryOperator<KllFloatsSketch> {

    @Override
    protected KllFloatsSketch _apply(final KllFloatsSketch a, final KllFloatsSketch b) {
        a.merge(b);
        return a;
    }
}
