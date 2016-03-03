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
package gaffer.function.simple.aggregate;

import gaffer.function.SimpleAggregateFunction;
import gaffer.function.annotation.Inputs;
import gaffer.function.annotation.Outputs;
import gaffer.function.simple.types.FreqMap;

import java.util.Map.Entry;

/**
 * An <code>FreqMapAggregator</code> is a {@link SimpleAggregateFunction} that takes in
 * {@link gaffer.function.simple.types.FreqMap}s and merges the frequencies together.
 */
@Inputs(FreqMap.class)
@Outputs(FreqMap.class)
public class FreqMapAggregator extends SimpleAggregateFunction<FreqMap> {
    private FreqMap frequencyMap;

    @Override
    protected void _aggregate(final FreqMap input) {
        if (null != input) {
            if (null == frequencyMap) {
                frequencyMap = new FreqMap(input);
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
    protected FreqMap _state() {
        return frequencyMap != null ? frequencyMap : new FreqMap();
    }

    @Override
    public FreqMapAggregator statelessClone() {
        final FreqMapAggregator aggregator = new FreqMapAggregator();
        aggregator.init();
        return aggregator;
    }
}
