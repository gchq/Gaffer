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
package uk.gov.gchq.gaffer.sketches.datasketches.frequencies.binaryoperator;

import com.yahoo.sketches.frequencies.LongsSketch;

import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

/**
 * A {@code LongsSketchAggregator} is a {@link java.util.function.BinaryOperator} that takes in
 * {@link LongsSketch}s and merges them together using {@link LongsSketch#merge(LongsSketch)}.
 */
@Since("1.0.0")
@Summary("Aggregates LongSketches objects")
public class LongsSketchAggregator extends KorypheBinaryOperator<LongsSketch> {

    @Override
    protected LongsSketch _apply(final LongsSketch a, final LongsSketch b) {
        a.merge(b);
        return a;
    }
}
