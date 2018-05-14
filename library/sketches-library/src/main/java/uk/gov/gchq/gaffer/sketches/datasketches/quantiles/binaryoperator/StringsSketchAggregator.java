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

import com.google.common.collect.Ordering;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.quantiles.ItemsUnion;

import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

/**
 * A {@code StringsSketchAggregator} is a {@link java.util.function.BinaryOperator} that aggregates
 * {@link ItemsSketch}s of {@link String}s using an {@link ItemsUnion}.
 */
@Since("1.0.0")
@Summary("Aggregates ItemsSketches of Strings using an ItemsUnion")
public class StringsSketchAggregator extends KorypheBinaryOperator<ItemsSketch<String>> {

    @Override
    protected ItemsSketch<String> _apply(final ItemsSketch<String> a, final ItemsSketch<String> b) {
        final ItemsUnion<String> union = ItemsUnion.getInstance(Ordering.<String>natural());
        union.update(a);
        union.update(b);
        return union.getResult();
    }
}
