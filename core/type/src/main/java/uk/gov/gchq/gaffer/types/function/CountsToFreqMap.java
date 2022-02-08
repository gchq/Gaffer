/*
 * Copyright 2016-2022 Crown Copyright
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

import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.function.KorypheFunction;

import java.util.Iterator;
import java.util.List;


/**
 * CountsToFreqMap is a KorypheFunction which takes an array of counts and converts them
 * to a FreqMap according to a predefined array of Keys. For this to work, the length of
 * the two arrays must match. If either the keys or input data is null, an empty frequency
 * map will be returned.
 */
@Since("2.0.0")
@Summary("Converts an iterable to a Frequency Map")
public class CountsToFreqMap extends KorypheFunction<List<Long>, FreqMap> {
    private List<String> freqMapKeys;

    public CountsToFreqMap() {
        // For Json Serialisation;
    }

    public CountsToFreqMap(final List<String> freqMapKeys) {
        this.freqMapKeys = freqMapKeys;
    }

    @Override
    public FreqMap apply(final List<Long> counts) {
        FreqMap freqMap = new FreqMap();

        if (this.freqMapKeys == null || counts == null) {
            return freqMap;
        }
        Iterator<String> keysIterator = freqMapKeys.iterator();
        Iterator<Long> valuesIterator = counts.iterator();

        while (keysIterator.hasNext() && valuesIterator.hasNext()) {
            freqMap.upsert(keysIterator.next(), valuesIterator.next());
        }

        return freqMap;
    }

    public Iterable<String> getFreqMapKeys() {
        return freqMapKeys;
    }

    public void setFreqMapKeys(final List<String> freqMapKeys) {
        this.freqMapKeys = freqMapKeys;
    }

    public CountsToFreqMap freqMapKeys(final List<String> freqMapKeys) {
        this.freqMapKeys = freqMapKeys;
        return this;
    }
}
