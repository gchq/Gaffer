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
package uk.gov.gchq.gaffer.example.function.filter;

import uk.gov.gchq.gaffer.function.filter.MapContains;
import java.util.HashMap;
import java.util.Map;

public class MapContainsExample extends FilterFunctionExample {
    private final Map<String, String> map = new HashMap<>();
    final Map<String, String> mapNoA = new HashMap<>();
    final Map<String, String> mapNullA = new HashMap<>();

    public static void main(final String[] args) {
        new MapContainsExample().run();
    }

    public MapContainsExample() {
        super(MapContains.class);

        map.put("a", "1");
        map.put("b", "2");
        map.put("c", "3");

        mapNoA.put("b", "2");
        mapNoA.put("c", "3");

        mapNullA.put("a", null);
        mapNullA.put("b", "2");
        mapNullA.put("c", "3");
    }

    public void runExamples() {
        mapContains();
    }

    public void mapContains() {
        // ---------------------------------------------------------
        final MapContains function = new MapContains("a");
        // ---------------------------------------------------------

        runExample(function, map, mapNoA, mapNullA);
    }
}
