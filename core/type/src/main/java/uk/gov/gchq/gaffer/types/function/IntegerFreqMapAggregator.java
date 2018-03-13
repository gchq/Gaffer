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
package uk.gov.gchq.gaffer.types.function;

import uk.gov.gchq.gaffer.types.IntegerFreqMap;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

import java.util.Map.Entry;

/**
 * An {@code FreqMapAggregator} is a {@link KorypheBinaryOperator} that takes in
 * {@link IntegerFreqMap}s and merges the frequencies together.
 *
 * @deprecated use {@link uk.gov.gchq.gaffer.types.IntegerFreqMap} with {@link FreqMapAggregator}.
 */
@Deprecated
@Since("1.0.0")
public class IntegerFreqMapAggregator extends KorypheBinaryOperator<IntegerFreqMap> {
    @Override
    public IntegerFreqMap _apply(final IntegerFreqMap a, final IntegerFreqMap b) {
        for (final Entry<String, Integer> entry : b.entrySet()) {
            if (a.containsKey(entry.getKey())) {
                a.put(entry.getKey(), a.get(entry.getKey()) + entry.getValue());
            } else {
                a.put(entry.getKey(), entry.getValue());
            }
        }

        return a;
    }
}
