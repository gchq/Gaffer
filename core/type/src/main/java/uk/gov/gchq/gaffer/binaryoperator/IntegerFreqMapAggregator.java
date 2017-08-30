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
package uk.gov.gchq.gaffer.binaryoperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.types.IntegerFreqMap;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;
import java.util.Map.Entry;

/**
 * An <code>FreqMapAggregator</code> is a {@link KorypheBinaryOperator} that takes in
 * {@link IntegerFreqMap}s and merges the frequencies together.
 *
 * @deprecated use {@link uk.gov.gchq.gaffer.types.IntegerFreqMap} with {@link FreqMapAggregator}.
 */
@Deprecated
public class IntegerFreqMapAggregator extends KorypheBinaryOperator<IntegerFreqMap> {
    private static final Logger LOGGER = LoggerFactory.getLogger(IntegerFreqMapAggregator.class);

    @Override
    public IntegerFreqMap _apply(final IntegerFreqMap a, final IntegerFreqMap b) {
       return _apply(a, b, true);
    }

    /**
     * Includes logic on whether or not to truncate data if the resulting {@link IntegerFreqMap} would be too large.
     * If truncate is set to true, as many entries as possible will be merged, but consequently data may be lost.
     * If it is set to false, and the result would be too large, the merge will not be attempted.
     * @param a an IntegerFreqMap object
     * @param b another IntegerFreqMap to be merged
     * @param truncate true by default, specifies whether or not data should be truncated
     * @return an aggregated IntegerFreqMap if possible, else the first input if the merge would be unsafe.
     */
    public IntegerFreqMap _apply(final IntegerFreqMap a, final IntegerFreqMap b, final boolean truncate) {
        final int MAX_SIZE = 1073741824;        // From HashMap - MAXIMUM_CAPACITY
        if (a.size() + b.size() > MAX_SIZE) {
            if(!truncate) {
                LOGGER.warn("Max size of: " + MAX_SIZE + " potentially exceeded - to avoid truncation, the first input will be returned.");
                return a;
            }
            LOGGER.warn("Max size of: " + MAX_SIZE + " potentially exceeded - data may be lost due to truncation.");
        }
        for (final Entry<String, Integer> entry : b.entrySet()) {
            if (a.containsKey(entry.getKey())) {
                a.put(entry.getKey(), a.get(entry.getKey()) + entry.getValue());
            } else if (a.size() < MAX_SIZE) {
                a.put(entry.getKey(), entry.getValue());
            }
        }
        return a;
    }
}
