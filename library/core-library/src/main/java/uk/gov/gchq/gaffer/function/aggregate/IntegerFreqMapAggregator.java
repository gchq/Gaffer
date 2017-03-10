/*
 * Copyright 2016 Crown Copyright
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
package uk.gov.gchq.gaffer.function.aggregate;

import uk.gov.gchq.gaffer.types.IntegerFreqMap;
import uk.gov.gchq.koryphe.binaryoperator.KorpheBinaryOperator;
import java.util.Map.Entry;
import java.util.function.BinaryOperator;

/**
 * An <code>FreqMapAggregator</code> is a {@link BinaryOperator} that takes in
 * {@link IntegerFreqMap}s and merges the frequencies together.
 *
 * @deprecated use {@link uk.gov.gchq.gaffer.types.IntegerFreqMap} with {@link FreqMapAggregator}.
 */
@Deprecated
public class IntegerFreqMapAggregator extends KorpheBinaryOperator<IntegerFreqMap> {
    @Override
    public IntegerFreqMap apply(final IntegerFreqMap input1, final IntegerFreqMap input2) {
        if (null == input1) {
            return new IntegerFreqMap(input2);
        }

        if (null == input2) {
            return new IntegerFreqMap(input1);
        }

        final IntegerFreqMap result = new IntegerFreqMap(input1);
        for (final Entry<String, Integer> entry : input2.entrySet()) {
            if (result.containsKey(entry.getKey())) {
                result.put(entry.getKey(), result.get(entry.getKey()) + entry.getValue());
            } else {
                result.put(entry.getKey(), entry.getValue());
            }
        }

        return result;
    }
}
