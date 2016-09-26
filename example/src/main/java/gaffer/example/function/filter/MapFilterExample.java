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
package gaffer.example.function.filter;

import gaffer.function.MapFilter;
import gaffer.function.simple.filter.Exists;
import gaffer.function.simple.filter.IsMoreThan;
import gaffer.types.simple.FreqMap;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class MapFilterExample extends FilterFunctionExample {
    public static void main(final String[] args) {
        new MapFilterExample().run();
    }

    public MapFilterExample() {
        super(MapFilter.class);
    }

    public void runExamples() {
        freqMapIsMoreThan2();
        freqMapIsMoreThanOrEqualTo2();
        mapWithDateKeyHasAValueThatExists();
    }

    public void freqMapIsMoreThan2() {
        final FreqMap map1 = new FreqMap();
        map1.put("key1", 1);

        final FreqMap map2 = new FreqMap();
        map2.put("key1", 2);

        final FreqMap map3 = new FreqMap();
        map3.put("key1", 3);

        final FreqMap map4 = new FreqMap();
        map4.put("key1", 3);
        map4.put("key2", 0);

        final FreqMap map5 = new FreqMap();
        map5.put("key2", 3);

        runExample(new MapFilter("key1", new IsMoreThan(2)),
                "new MapFilter(\"key1\", new IsMoreThan(2))",
                map1, map2, map3, map4, map5);
    }

    public void freqMapIsMoreThanOrEqualTo2() {
        final FreqMap map1 = new FreqMap();
        map1.put("key1", 1);

        final FreqMap map2 = new FreqMap();
        map2.put("key1", 2);

        final FreqMap map3 = new FreqMap();
        map3.put("key1", 3);

        final FreqMap map4 = new FreqMap();
        map4.put("key1", 3);
        map4.put("key2", 0);

        final FreqMap map5 = new FreqMap();
        map5.put("key2", 3);

        runExample(new MapFilter("key1", new IsMoreThan(2, true)),
                "new MapFilter(\"key1\", new IsMoreThan(2, true))",
                map1, map2, map3, map4, map5);
    }

    public void mapWithDateKeyHasAValueThatExists() {
        final Map<Date, Long> map1 = new HashMap<>();
        map1.put(new Date(0L), 1L);

        final Map<Date, Long> map2 = new HashMap<>();
        map2.put(new Date(), 2L);

        runExample(new MapFilter(new Date(0L), new Exists()),
                "new MapFilter(new Date(0L), new Exists())",
                map1, map2);
    }
}
