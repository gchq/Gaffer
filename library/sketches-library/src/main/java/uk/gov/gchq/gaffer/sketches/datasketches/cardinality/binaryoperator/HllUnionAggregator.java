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
package uk.gov.gchq.gaffer.sketches.datasketches.cardinality.binaryoperator;

import com.yahoo.sketches.hll.Union;

import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

/**
 * A {@code HllUnionAggregator} is a {@link java.util.function.BinaryOperator} that takes in
 * {@link com.yahoo.sketches.hll.Union}s and merges them together using
 * {@link Union#update(com.yahoo.sketches.hll.HllSketch)}.
 */
@Since("1.0.0")
@Summary("Aggregates HllUnions objects")
public class HllUnionAggregator extends KorypheBinaryOperator<Union> {

    @Override
    protected Union _apply(final Union a, final Union b) {
        a.update(b.getResult());
        return a;
    }
}
