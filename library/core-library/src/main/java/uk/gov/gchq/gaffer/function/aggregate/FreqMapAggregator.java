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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.function.SimpleAggregateFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;
import uk.gov.gchq.gaffer.function.annotation.Outputs;
import uk.gov.gchq.gaffer.types.FreqMap;
import java.util.Map.Entry;

/**
 * An <code>FreqMapAggregator</code> is a {@link SimpleAggregateFunction} that takes in
 * {@link FreqMap}s and merges the frequencies together.
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
                for (final Entry<String, Long> entry : input.entrySet()) {
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
        return frequencyMap;
    }

    @Override
    public FreqMapAggregator statelessClone() {
        final FreqMapAggregator aggregator = new FreqMapAggregator();
        aggregator.init();
        return aggregator;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final FreqMapAggregator that = (FreqMapAggregator) o;

        return new EqualsBuilder()
                .append(inputs, that.inputs)
                .append(outputs, that.outputs)
                .append(frequencyMap, that.frequencyMap)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(inputs)
                .append(outputs)
                .append(frequencyMap)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("inputs", inputs)
                .append("outputs", outputs)
                .append("frequencyMap", frequencyMap)
                .toString();
    }
}
