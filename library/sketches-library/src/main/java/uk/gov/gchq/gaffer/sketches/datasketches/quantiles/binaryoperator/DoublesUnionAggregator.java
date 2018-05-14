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

import com.yahoo.sketches.quantiles.DoublesUnion;

import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

/**
 * A {@code DoublesUnionAggregator} is a {@link java.util.function.BinaryOperator} that aggregates {@link DoublesUnion}s.
 * It does this by extracting a {@link com.yahoo.sketches.quantiles.DoublesSketch} from each {@link DoublesUnion}
 * and merges that using {@link DoublesUnion#update(com.yahoo.sketches.quantiles.DoublesSketch)}.
 */
@Since("1.0.0")
@Summary("Aggregates DoublesUnions")
public class DoublesUnionAggregator extends KorypheBinaryOperator<DoublesUnion> {

    @Override
    protected DoublesUnion _apply(final DoublesUnion a, final DoublesUnion b) {
        a.update(b.getResult());
        return a;
    }
}
