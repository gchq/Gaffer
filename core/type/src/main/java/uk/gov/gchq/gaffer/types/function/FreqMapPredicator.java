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
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.function.KorypheFunction;

import java.util.function.BiPredicate;
import java.util.function.Predicate;

/**
 * A {@code FreqMapPredicator} is a {@link KorypheFunction} that extracts a
 * a clone of the current frequency map provided a valid predicate or bipredicate.
 */
@Since("1.7.0")
@Summary("Returns a frequency map based on the predicate provided")
public class FreqMapPredicator extends KorypheFunction<FreqMap, FreqMap> {

    private BiPredicate<String, Long> predicate;

    /**
     * Constructor for FreqMapPredicator.<br>
     * If null supplied as predicate then {@link FreqMapPredicator#apply(FreqMap)} will yield null.
     *
     * @param predicate The predicate for the key constraints of the map.
     */
    public FreqMapPredicator(final Predicate<String> predicate) {
        if (predicate != null) {
            this.predicate = (s, aLong) -> predicate.test(s);
        }
    }

    /**
     * Constructor for FreqMapPredicator.<br>
     * The predicate provided in this constructor does not need to utilize both
     * key and value for testing.<br>
     * If null supplied as predicate then {@link FreqMapPredicator#apply(FreqMap)} will yield null.
     *
     * @param predicate The predicate for both key and value constraints.
     */
    public FreqMapPredicator(final BiPredicate<String, Long> predicate) {
        if (predicate != null) {
            this.predicate = predicate;
        }
    }

    /**
     * Creates a filtered copy of the map using a supplied predicate.<br>
     * Returns null if predicate supplied is null.
     *
     * @param map The frequency map that is to be sorted through
     * @return A new frequency map with only the filtered entries present.
     */
    private FreqMap filterPredicate(final FreqMap map) {
        if (predicate == null) {
            return null;
        }

        final FreqMap f = new FreqMap();

        map.entrySet().stream().filter(e -> predicate.test(e.getKey(), e.getValue()))
                .forEach(e -> f.upsert(e.getKey(), e.getValue()));

        return f;
    }

    @Override
    public FreqMap apply(final FreqMap freqMap) {
        return filterPredicate(freqMap);
    }
}
