/*
 * Copyright 2016-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.sketches.clearspring.cardinality.binaryoperator;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

/**
 * A {@code HyperLogLogPlusAggregator} is a {@link java.util.function.BinaryOperator} that takes in
 * {@link HyperLogLogPlus}s and merges the sketches together.
 */
@Since("1.0.0")
public class HyperLogLogPlusAggregator extends KorypheBinaryOperator<HyperLogLogPlus> {
    @Override
    protected HyperLogLogPlus _apply(final HyperLogLogPlus a, final HyperLogLogPlus b) {
        try {
            a.addAll(b);
        } catch (final CardinalityMergeException exception) {
            throw new RuntimeException("An Exception occurred when trying to aggregate the HyperLogLogPlus objects", exception);
        }
        return a;
    }
}
