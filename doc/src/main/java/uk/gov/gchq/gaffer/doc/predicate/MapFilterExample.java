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
package uk.gov.gchq.gaffer.doc.predicate;

import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.koryphe.impl.predicate.Exists;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.predicate.PredicateMap;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class MapFilterExample extends PredicateExample {
    public static void main(final String[] args) {
        new MapFilterExample().run();
    }

    public MapFilterExample() {
        super(PredicateMap.class);
    }

    @Override
    public void runExamples() {
        freqMapIsMoreThan2();
        freqMapIsMoreThanOrEqualTo2();
        mapWithDateKeyHasAValueThatExists();
    }

    public void freqMapIsMoreThan2() {
        // ---------------------------------------------------------
        final PredicateMap function = new PredicateMap("key1", new IsMoreThan(2L));
        // ---------------------------------------------------------

        final FreqMap map1 = new FreqMap();
        map1.put("key1", 1L);

        final FreqMap map2 = new FreqMap();
        map2.put("key1", 2L);

        final FreqMap map3 = new FreqMap();
        map3.put("key1", 3L);

        final FreqMap map4 = new FreqMap();
        map4.put("key1", 3L);
        map4.put("key2", 0L);

        final FreqMap map5 = new FreqMap();
        map5.put("key2", 3L);

        runExample(function, map1, map2, map3, map4, map5);
    }

    public void freqMapIsMoreThanOrEqualTo2() {
        // ---------------------------------------------------------
        final PredicateMap function = new PredicateMap("key1", new IsMoreThan(2L, true));
        // ---------------------------------------------------------

        final FreqMap map1 = new FreqMap();
        map1.put("key1", 1L);

        final FreqMap map2 = new FreqMap();
        map2.put("key1", 2L);

        final FreqMap map3 = new FreqMap();
        map3.put("key1", 3L);

        final FreqMap map4 = new FreqMap();
        map4.put("key1", 3L);
        map4.put("key2", 0L);

        final FreqMap map5 = new FreqMap();
        map5.put("key2", 3L);

        runExample(function, map1, map2, map3, map4, map5);
    }

    public void mapWithDateKeyHasAValueThatExists() {
        // ---------------------------------------------------------
        final PredicateMap function = new PredicateMap(new Date(0L), new Exists());
        // ---------------------------------------------------------

        final Map<Date, Long> map1 = new HashMap<>();
        map1.put(new Date(0L), 1L);

        final Map<Date, Long> map2 = new HashMap<>();
        map2.put(new Date(), 2L);

        runExample(function, map1, map2);
    }
}
