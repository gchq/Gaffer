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

import com.yahoo.sketches.frequencies.ItemsSketch;

import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

/**
 * A {@code StringsSketchAggregator} is a {@link java.util.function.BinaryOperator} that takes in
 * {@link ItemsSketch}s of {@link String}s and merges them together using {@link ItemsSketch#merge(ItemsSketch)}.
 * <p>
 * NB: We cannot provide a generic aggregator for any type T as we need to clone the first sketch that is
 * supplied to the {@code _aggregate} method and that requires serialising and deserialising which
 * requires a specific serialiser.
 */
@Since("1.0.0")
@Summary("Aggregates ItemSketches of Strings")
public class StringsSketchAggregator extends KorypheBinaryOperator<ItemsSketch<String>> {

    @Override
    protected ItemsSketch<String> _apply(final ItemsSketch<String> a, final ItemsSketch<String> b) {
        a.merge(b);
        return a;
    }
}
