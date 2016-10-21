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
package uk.gov.gchq.gaffer.function.simple.aggregate;

import uk.gov.gchq.gaffer.function.SimpleAggregateFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;
import uk.gov.gchq.gaffer.function.annotation.Outputs;
import uk.gov.gchq.gaffer.types.simple.IntegerFreqMap;
import java.util.Map.Entry;

/**
 * An <code>FreqMapAggregator</code> is a {@link SimpleAggregateFunction} that takes in
 * {@link IntegerFreqMap}s and merges the frequencies together.
 *
 * @deprecated use {@link gaffer.types.simple.FreqMap} with {@link FreqMapAggregator}.
 */
@Inputs(IntegerFreqMap.class)
@Outputs(IntegerFreqMap.class)
@Deprecated
public class IntegerFreqMapAggregator extends SimpleAggregateFunction<IntegerFreqMap> {
    private IntegerFreqMap frequencyMap;

    @Override
    protected void _aggregate(final IntegerFreqMap input) {
        if (null != input) {
            if (null == frequencyMap) {
                frequencyMap = new IntegerFreqMap(input);
            } else {
                for (Entry<String, Integer> entry : input.entrySet()) {
                    if (frequencyMap.containsKey(entry.getKey())) {
                        frequencyMap.put(entry.getKey(), frequencyMap.get(entry.getKey()) + entry.getValue());
                    } else {
                        frequencyMap.put(entry.getKey(), entry.getValue());
                    }
                }
            }
        }
    }

    @Override
    public void init() {
        frequencyMap = null;
    }

    @Override
    protected IntegerFreqMap _state() {
        return frequencyMap;
    }

    @Override
    public IntegerFreqMapAggregator statelessClone() {
        final IntegerFreqMapAggregator aggregator = new IntegerFreqMapAggregator();
        aggregator.init();
        return aggregator;
    }
}
