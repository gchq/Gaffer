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
package gaffer.function.simple.filter;

import gaffer.function.SimpleFilterFunction;
import gaffer.function.annotation.Inputs;
import gaffer.types.simple.FreqMap;

/**
 * An <code>FreqMapIsMoreThan</code> is a {@link SimpleFilterFunction} that checks
 * whether the frequency value associated with a specfific {@link gaffer.types.simple.FreqMap}
 * key exists and is greater than a provided value.
 */
@Inputs(FreqMap.class)
public class FreqMapIsMoreThan extends SimpleFilterFunction<FreqMap> {
    private String key;
    private int frequency;
    private boolean orEqualTo;

    public FreqMapIsMoreThan() {
        // Required for serialisation
    }

    public FreqMapIsMoreThan(final String key) {
        this(key, 0, false);
    }

    public FreqMapIsMoreThan(final String key, final int frequency) {
        this(key, frequency, false);
    }

    public FreqMapIsMoreThan(final String key, final int frequency, final boolean orEqualTo) {
        this.key = key;
        this.frequency = frequency;
        this.orEqualTo = orEqualTo;
    }

    public String getKey() {
        return key;
    }

    public void setKey(final String key) {
        this.key = key;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(final int frequency) {
        this.frequency = frequency;
    }

    public boolean getOrEqualTo() {
        return orEqualTo;
    }

    public void setOrEqualTo(final boolean orEqualTo) {
        this.orEqualTo = orEqualTo;
    }

    public FreqMapIsMoreThan statelessClone() {
        return new FreqMapIsMoreThan(key, frequency, orEqualTo);
    }

    @Override
    public boolean isValid(final FreqMap input) {
        final Integer inputFreq = input.get(key);
        if (null != inputFreq) {
            if (orEqualTo) {
                return inputFreq >= frequency;
            }

            return inputFreq > frequency;
        }

        return false;
    }
}
