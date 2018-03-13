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

import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.function.KorypheFunction;

/**
 * A {@code FreqMapExtractor} is a {@link KorypheFunction} that extracts a
 * count from a frequency map for the provided key.
 */
@Since("1.0.0")
public class FreqMapExtractor extends KorypheFunction<FreqMap, Long> {
    private String key;

    public FreqMapExtractor() {
    }

    public FreqMapExtractor(final String key) {
        this.key = key;
    }

    @Override
    public Long apply(final FreqMap freqMap) {
        if (null != freqMap) {
            return freqMap.get(key);
        }

        return null;
    }

    public String getKey() {
        return key;
    }

    public void setKey(final String key) {
        this.key = key;
    }
}
